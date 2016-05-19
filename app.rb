require 'rubygems'
require 'redis'
require 'open-uri'
require 'nokogiri'
require 'fileutils'
require 'zip'
require 'yaml'

# get parameters from config file
config = YAML.load_file('runtime.yaml')

puts "Establishing Redis connection"
# init Redis connection
base = Redis.new( :host => config[:redis][:host],
                  :port => config[:redis][:port],
                  :db   => config[:redis][:db])

# creating http connection
puts "Getting files list to download"
page = Nokogiri::HTML(open(config[:source][:url]))
file_names = []

# getting array of zipped files
page.css('tr td a').each do |element|
  if element.text == element['href']
    file_names.push(element.text)
  end
end

# create temporary directory for unzipped xml files
directory_name = "tmp"
Dir.mkdir(directory_name) unless File.exists?(directory_name)

# main files processing
file_names.each do |zipfile|

  archive = "tmp/#{zipfile}"
  # skip the file in case it's presence because other instance of app processing it
  next if File.exists?(archive)

  # downloading file to hard drive
  puts "Downloading #{zipfile} file"
  open(archive, "wb") do |file|
    Net::HTTP.start("feed.omgili.com") do |http|
      resp = http.get("/5Rh5AMTrc4Pv/mainstream/posts/#{zipfile}")
      file.write(resp.body)
    end
  end

  dest_dir = "tmp/#{File.basename(zipfile, '.zip')}"
  Dir.mkdir(dest_dir) unless File.exists?(dest_dir)

  # unpuck xml files and save content to Redis
  Zip::File.open(archive) do |file|
    file.each do |entry|

      dest_file_abs_path = File.expand_path("#{dest_dir}/#{entry.name}")
      unless File.exists?(dest_file_abs_path)
        puts "Extracting #{entry.name}"

        # extract xml file if not extracted already
        entry.extract(dest_file_abs_path)
      end
      # read file content
      content = File.read(dest_file_abs_path).strip()

      # checking if list not empty
      if base.llen(config[:redis][:entry]) > 0
        puts 'Checking entries for duplicates'
        # get etire list from Redis
        list = base.lrange(config[:redis][:entry], 0, -1)

        # looking for elemnt in Redis list and skip it if present
        if list.index(content) != nil
            puts 'Duplicated record found.'
            next
        end
      end

      puts "Pushing #{entry.name} into the Redis."
      base.rpush(config[:redis][:entry], content)

    end
  end

end

puts "Done."
