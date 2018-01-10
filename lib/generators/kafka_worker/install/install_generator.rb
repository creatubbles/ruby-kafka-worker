class KafkaWorker::InstallGenerator < ::Rails::Generators::Base
  source_root File.expand_path('../templates', __FILE__)

  def copy_rails_files
    directory 'app/kafka_handlers'
    template 'bin/kafka_worker'
    chmod 'bin/kafka_worker', 0755
  end
end
