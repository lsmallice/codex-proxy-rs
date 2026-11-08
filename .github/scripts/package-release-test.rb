require 'minitest/autorun'
require 'open3'
require 'rbconfig'
require 'tmpdir'
require 'yaml'
require_relative 'package-release'
require_relative 'release-metadata'

# 以临时制品验证发行包、镜像与安装入口，不构建程序或访问真实发行服务
class ReleasePackageTest < Minitest::Test
  ROOT = File.expand_path('../..', __dir__)
  REPOSITORY = 'lsmallice/codex-proxy-rs'
  VERSION = '99.0.0'

  def setup
    @directory = Dir.mktmpdir('release-package-')
    @original = Dir.pwd
    Dir.chdir(@directory)
    %w[deploy/compose.yaml deploy/config.example.yaml deploy/install.sh
       backend/crates/gateway-plugin/runtime/plugin-host-compatibility.json].each do |path|
      FileUtils.mkdir_p(File.dirname(path))
      FileUtils.cp(File.join(ROOT, path), path)
    end
    @platforms = YAML.load_file(File.join(ROOT, 'release/platforms.yaml')).fetch('platforms')
    @platforms.each do |platform|
      directory = "dist/binaries/codex-proxy-rs-#{platform.fetch('os')}-#{platform.fetch('arch')}"
      FileUtils.mkdir_p(directory)
      File.write("#{directory}/#{platform.fetch('binary')}", "binary-#{platform.fetch('target')}")
    end
    FileUtils.mkdir_p('dist/frontend/assets')
    File.write('dist/frontend/index.html', '<html>fixture</html>')
    File.write('dist/frontend/assets/app.js', 'console.log("fixture");')
    ReleaseMetadata.seal_manifest('version' => VERSION, 'git_sha' => 'a' * 40)
    @manifest = File.binread(ReleasePackage::MANIFEST)
    workflow = YAML.load_file(File.join(ROOT, '.github/workflows/release.yml'))
    @environment = workflow.fetch('env').merge(
      'VERSION' => VERSION, 'DOCKER_VERSION' => VERSION,
      'PLATFORM_MATRIX' => JSON.generate('include' => @platforms)
    )
  end

  def teardown
    Dir.chdir(@original)
    FileUtils.chmod_R('u+w', @directory)
    FileUtils.rm_rf(@directory)
  end

  def package(command, environment = {})
    Open3.capture3(@environment.merge(environment), RbConfig.ruby,
                  File.join(ROOT, '.github/scripts/package-release.rb'), command)
  end

  def package!(command, environment = {})
    output, error, status = package(command, environment)
    assert status.success?, "#{command}: #{output}#{error}"
  end

  def compose_service
    FileUtils.cp('dist/release/config.example.yaml', 'dist/release/config.yaml')
    output, error, status = Open3.capture3(
      { 'CPR_IMAGE' => nil }, 'docker', 'compose', '-f', 'dist/release/compose.yaml',
      'config', '--format', 'json'
    )
    assert status.success?, error
    JSON.parse(output).fetch('services').fetch('codex-proxy-rs')
  ensure
    FileUtils.rm_f('dist/release/config.yaml')
  end

  def test_archives_pin_fork_image_and_preserve_complete_release_identity
    [VERSION, "#{VERSION}-rc.1"].each do |version|
      ReleaseMetadata.seal_manifest('version' => version, 'git_sha' => 'a' * 40)
      @manifest = File.binread(ReleasePackage::MANIFEST)
      package!('archives', 'VERSION' => version, 'DOCKER_VERSION' => version)
      compose = compose_service
      assert_equal "ghcr.io/#{REPOSITORY}:#{version}", compose.fetch('image')
      assert_equal REPOSITORY, compose.fetch('environment').fetch('CPR_UPDATE_REPOSITORY')
      checksums = File.readlines('dist/release/checksums.txt').to_h do |line|
        digest, file = line.strip.split(/\s+/, 2)
        [file, digest]
      end
      assets = Dir.children('dist/release') - ['checksums.txt']
      assert_equal assets.sort, checksums.keys.sort
      assets.each { |file| assert_equal Digest::SHA256.file("dist/release/#{file}").hexdigest, checksums.fetch(file) }
      @platforms.each do |platform|
        archive = "dist/release/codex-proxy-rs_#{version}_#{platform['os']}_#{platform['arch']}.tar.gz"
        contents = {
          platform.fetch('binary') => "binary-#{platform.fetch('target')}",
          'web/dist/index.html' => File.binread('dist/frontend/index.html'),
          'web/dist/assets/app.js' => File.binread('dist/frontend/assets/app.js'),
          'deploy/config.example.yaml' => File.binread('deploy/config.example.yaml'),
          'plugins/official/plugin-release-manifest.json' => @manifest
        }
        contents.each do |path, expected|
          output, error, status = Open3.capture3('tar', '-xOzf', archive, "codex-proxy-rs/#{path}", binmode: true)
          assert status.success?, error
          assert_equal expected, output
        end
      end
    end
  end

  def test_image_uses_same_binary_and_sealed_manifest
    @platforms.select { |platform| platform['docker'] }.each do |platform|
      name = "#{platform['os']}-#{platform['arch']}"
      FileUtils.mkdir_p('dist/docker-artifacts')
      FileUtils.cp_r("dist/binaries/codex-proxy-rs-#{name}", 'dist/docker-artifacts')
      package!('image', 'MATRIX_OS' => platform['os'], 'MATRIX_ARCH' => platform['arch'],
                       'MATRIX_BINARY' => platform['binary'])
      root = "dist/docker/#{name}"
      assert_equal "binary-#{platform['target']}", File.binread("#{root}/codex-proxy-rs")
      assert File.executable?("#{root}/codex-proxy-rs")
      assert_equal @manifest, File.binread("#{root}/plugins/official/plugin-release-manifest.json")
      assert_equal 0o444, File.stat("#{root}/plugins/official/plugin-release-manifest.json").mode & 0o777
    end
  end

  def test_packaging_rejects_wrong_namespace_or_unsealed_manifest
    _, error, status = package('archives', 'IMAGE_NAME' => 'other/repository')
    refute status.success?
    assert_includes error, 'Expected exactly one default application image'
    manifest = JSON.parse(@manifest).merge('sealed' => false)
    File.write(ReleasePackage::MANIFEST, JSON.generate(manifest))
    _, error, status = package('archives')
    refute status.success?
    assert_includes error, 'Expected a sealed host manifest'
  end

  def test_installer_downloads_deployment_files_from_one_fork_release
    package!('archives')
    FileUtils.mkdir_p('bin')
    File.write('bin/curl', <<~RUBY)
      #!/usr/bin/env ruby
      require 'json'
      url = ARGV.find { |value| value.start_with?('https://') }
      File.open(ENV.fetch('CURL_LOG'), 'a') { |file| file.puts(JSON.generate(url)) }
      if ARGV.include?('-w')
        print "https://github.com/#{REPOSITORY}/releases/tag/v#{VERSION}"
      else
        destination = ARGV.fetch(ARGV.index('--output') + 1)
        File.binwrite(destination, File.binread(File.join(ENV.fetch('ASSETS'), File.basename(url))))
      end
    RUBY
    File.chmod(0o755, 'bin/curl')
    environment = {
      'PATH' => "#{@directory}/bin:#{ENV.fetch('PATH')}",
      'CURL_LOG' => "#{@directory}/curl.jsonl", 'ASSETS' => "#{@directory}/dist/release",
      'INSTALL_DIR' => "#{@directory}/install", 'CPR_RELEASE_TAG' => ''
    }
    script = 'source "$1"; mkdir -p "$DEPLOY_DIR"; download_release_files'
    output, error, status = Open3.capture3(environment, 'bash', '-c', script, 'installer-test', 'deploy/install.sh')
    assert status.success?, "#{output}#{error}"
    assert_equal [
      "https://github.com/#{REPOSITORY}/releases/latest",
      "https://github.com/#{REPOSITORY}/releases/download/v#{VERSION}/compose.yaml",
      "https://github.com/#{REPOSITORY}/releases/download/v#{VERSION}/config.example.yaml"
    ], File.readlines('curl.jsonl').map { |line| JSON.parse(line) }
    %w[compose.yaml config.example.yaml].each do |file|
      assert_equal File.binread("dist/release/#{file}"), File.binread("install/deploy/#{file}")
    end
  end
end
