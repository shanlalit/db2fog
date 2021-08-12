# coding: utf-8

require 'active_support'
require 'active_support/core_ext/class/attribute_accessors'
require 'active_support/core_ext/hash/except'
require 'fog/core'
require 'tempfile'
require 'fileutils'
require 'db2fog/railtie'

class DB2Fog
  cattr_accessor :config

  def backup
    file_name = "dump-#{db_credentials[:database]}-#{Time.now.utc.strftime("%Y%m%d%H%M")}.sql.gz"
    local_dump_path = database.dump
    store.store(file_name, open(local_dump_path))
    store.store(most_recent_dump_file_name, file_name)
  ensure
    FileUtils.rm(local_dump_path) if File.exists?(local_dump_path)
  end

  def restore(environment = nil)
    dump_file_name = store.fetch(most_recent_dump_file_name(environment)).read
    file = store.fetch(dump_file_name)
    database.restore(file.path)
  end

  def clean
    to_keep = []
    # only consider files that belong to db2fog. Other files are ignored
    filelist = store.list.select {|file|
      file.include?(db_credentials[:database]) && file.match(/\d{12}.sql.gz\Z/)
    }
    files = filelist.map { |file|
      {
        :path => file,
        :date => Time.parse(file.split('-').last.split('.').first)
      }
    }
    # Keep all backups from the past day
    files.select {|x| x[:date] >= 1.day.ago }.each do |backup_for_day|
      to_keep << backup_for_day
    end

    # Keep one backup per day from the last week
    files.select {|x| x[:date] >= 1.week.ago }.group_by {|x| x[:date].strftime("%Y%m%d") }.values.each do |backups_for_last_week|
      to_keep << backups_for_last_week.sort_by{|x| x[:date].strftime("%Y%m%d") }.first
    end

    # Keep one backup per week since forever
    files.group_by {|x| x[:date].strftime("%Y%W") }.values.each do |backups_for_week|
      to_keep << backups_for_week.sort_by{|x| x[:date].strftime("%Y%m%d") }.first
    end

    to_destroy = filelist - to_keep.uniq.collect {|x| x[:path] }
    to_destroy.each do |file|
      store.delete(file.split('/').last)
    end
  end

  private

  def store
    @store ||= FogStore.new
  end

  def most_recent_dump_file_name(environment = nil)
    if environment
      raise "Unknown environment name (#{environment}). Check your database.yml" if Rails.configuration.database_configuration[environment].nil?
      db_name = Rails.configuration.database_configuration[environment]['database']
    else
      db_name = db_credentials[:database]
    end
    "most-recent-dump-#{db_name}.txt"
  end

  def db_credentials
    if Object.const_defined?(:ActiveRecord)
      ActiveRecord::Base.connection.instance_eval { @config } # Dodgy!
    elsif Object.const_defined?(:DataMapper)
      DataMapper.repository.adapter.options.inject({}){|m,(k,v)| m[k.to_sym] = v;m }
    elsif Object.const_defined?(:Sequel)
      opts = Sequel::DATABASES.first.opts
      opts[:username] = opts[:user]
      opts
    end
  end

  def database
    @database ||= case db_credentials[:adapter]
                  when /mysql/    then MysqlAdaptor.new(db_credentials)
                  when /postgres/ || /postgis/ then PsqlAdaptor.new(db_credentials)
                  else
                    raise "database adaptor '#{db_credentials[:adapter]}' not supported"
                  end
  end

  class BaseAdaptor

    def initialize(credentials)
      @credentials = credentials
    end

    def run(command)
      result = system(command)
      raise("error, process exited with status #{$?.exitstatus}") unless result
    end

    def dump
      dump_file = Tempfile.new(tempfile_prefix, DB2Fog.config[:local_dir])

      # Remove old dump files.
      # Not sure why they are being left behind... but they eventually use up all the disk space.
      other_dump_files = Dir.glob("#{File.dirname(dump_file.path)}/#{tempfile_prefix}*")
      other_dump_files.each do |file|
        if File.mtime(file) < 1.day.ago
          FileUtils.rm_f file
        end
      end

      dump_file.close
      run(dump_command(dump_file))
      dump_file.path
    rescue
      # Ensure a failed dump removes a partially complete dump file
      FileUtils.rm_f dump_file.path
      raise
    end

    def tempfile_prefix
      Digest::MD5.hexdigest("dump#{@credentials[:database]}/#{@credentials[:username]}/#{@credentials[:password]}")
    end

  end

  class MysqlAdaptor < BaseAdaptor

    def dump_command(dump_file)
      cmd = "mysqldump --quick --single-transaction --create-options #{mysql_options}"
      cmd += " | gzip -9 > #{dump_file.path}"
    end

    def restore(path)
      run "gunzip -c #{path} | mysql #{mysql_options}"
    end

    private

    def mysql_options
      cmd = ''
      cmd += " -u #{@credentials[:username]} " unless @credentials[:username].nil?
      cmd += " -p'#{@credentials[:password]}'" unless @credentials[:password].nil?
      cmd += " -h '#{@credentials[:host]}'"    unless @credentials[:host].nil?
      cmd += " --default-character-set=#{@credentials[:encoding]}" unless @credentials[:encoding].nil?
      cmd += " #{@credentials[:database]}"
    end

  end

  class PsqlAdaptor < BaseAdaptor

    def dump_command(dump_file)
      cmd = "pg_dump --clean --format=p --compress=1 #{pg_dump_options}"
      cmd += " > #{dump_file.path}"
    end

    def restore(path)
      run "gunzip -c #{path} | psql #{psql_options}"
    end

    private

    def pg_dump_options
      cmd = ''
      cmd += " -U #{@credentials[:username]} " unless @credentials[:username].nil?
      cmd += " -h '#{@credentials[:host]}'"    unless @credentials[:host].nil?
      cmd += " -w"                             if     pg_version >= 9
      cmd += " #{@credentials[:database]}"
    end

    def psql_options
      cmd = ''
      cmd += " -U #{@credentials[:username]} " unless @credentials[:username].nil?
      cmd += " -h '#{@credentials[:host]}'"    unless @credentials[:host].nil?
      cmd += " -w"                             if     pg_version >= 9
      cmd += " -d #{@credentials[:database]}"
    end

    def pg_version
      opts = database_options || {}
      opts[:pg_version] || 9
    end

    def database_options
      if DB2Fog.config.respond_to?(:[])
        DB2Fog.config[:database_options]
      else
        raise "DB2Fog not configured"
      end
    end

  end

  class FogStore

    def store(remote_filename, io)
      directory.files.create(:key => remote_filename, :body => io, :public => false)
    end

    def fetch(remote_filename)
      remote_file = directory.files.get(remote_filename)

      file = Tempfile.new("dump")
      open(file.path, 'wb') { |f| f.write(remote_file.body) }
      file
    end

    def list
      directory.files.map { |f| f.key }
    end

    def delete(remote_filename)
      remote_file = directory.files.head(remote_filename)
      remote_file.destroy if remote_file
    end

    private

    def fog_options
      if DB2Fog.config.respond_to?(:[])
        DB2Fog.config.except(:directory, :database_options)
      else
        raise "DB2Fog not configured"
      end
    end

    def directory_name
      if DB2Fog.config.respond_to?(:[])
        DB2Fog.config[:directory]
      else
        raise "DB2Fog not configured"
      end
    end

    def directory
      @directory ||= storage.directories.get(directory_name)
    end

    def storage
      @storage = Fog::Storage.new(fog_options)
    end
  end

end
