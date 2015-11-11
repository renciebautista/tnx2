# gem install timers
# gem install eventmachine

require 'socket'
require 'timers'
require 'thread'
require 'yaml'
require 'eventmachine'
require 'rexml/document'
require 'logger'
require 'mysql2'

include REXML

class Dispatch

	def initialize
		Thread::abort_on_exception = true

		mutex = Mutex.new

		@config = YAML.load_file('config/config.conf')

		@default_connect_time = @config['tigserver']['default_connect_time']
		@ips = @config['tigserver']['servers']

		@connect_time = @default_connect_time
		@connected = ""
		@last_update = Time.now

		@mutex = Mutex.new


		@log = Logger.new('logs/log.txt', 'daily')
		@queue = Queue.new

		@socket =  UDPSocket.new
		@socket.bind('0.0.0.0',30512)

		
		t1 = Thread.new{listen}
		t2 = Thread.new{mine}
		t3 = Thread.new{connect}
		t4 = Thread.new{broadcast}
		

		t1.join
		t2.join
		t3.join
		t4.join

		puts 'End.'
	end

	def connect
		puts 'Connecting....'
		EventMachine.run do
		  timer = EventMachine::PeriodicTimer.new(@default_connect_time) do
		  		data = "<?xml version=\"1.0\"?><Tig><Client.Connect Name=\"#{@config['tigserver']['name']}\" Version=\"#{@config['tigserver']['version']}\" /></Tig>"

			  	if (Time.now - @last_update) > @connect_time
			  		@connected = ""
			  		@connect_time = @default_connect_time
			  	end

			  	if @connected.empty?
				  	@ips.each do |ip|
				  		begin
				  			@socket.send(data, 0, ip.to_s, 30511)
				  		rescue Exception => e
				  			@connected = ""
				  			@log.error e.to_s + " - Connecting to TNX Server list."
				  		end
					end
				else
					begin
						@socket.send(data, 0, @connected.to_s, 30511)
					rescue Exception => e
						@connected = ""
						@log.error e.to_s + " - Connecting to TNX Server."
					end					
				end
		  	timer.interval = @connect_time
		  	end
		end
	end

	def listen
		puts	'Listening....'
		loop {
	    response = Thread.start(@socket.recvfrom(65536)) do |msg, sender| # each client thread
	    	@last_update = Time.now
	    	xml = Document.new(msg)
	    	xmldoc = XPath.match(xml, "Tig/Client.Connected")
	    
	    	if !xmldoc.empty?
	    		@connect_time = @default_connect_time
	    		if get_attribute(xml, "Client.Connected", "Success").to_s == "1"
	    			@mutex.synchronize{ @connected = sender[3].to_s }
	    			
	    			lifetime = get_attribute(xml, "Client.Connected", "Lifetime").to_s.to_i / 3
	    			if lifetime < 3
	    				@connect_time = @default_connect_time
	    			else
	    				@connect_time = lifetime
	    			end
	    		else
	    			@connect_time = @default_connect_time
	    		end
	    	end
	    	@queue << msg.to_s	
	    end

	  }
	end

	def mine
		puts 'Mining....'
		loop{
			# puts running_thread_count
			if !@queue.empty?

				value = @queue.pop
				# count = @queue.length
				# puts "consumed #{value}"
				sleep(1)
				# puts "count #{count}"
			end
		}
	end

	def broadcast
		puts 'Broadcasting.....'
		
		timers = Timers::Group.new
		every_five_seconds = timers.every(1) { 
			# puts running_thread_count
			begin
				@mutex.synchronize{ @connected }
				status = 0
				if !@connected.empty?
					status = 1
				end
				data = "tnx|#{@connected.to_s}|#{status}|#{@connect_time}"
				socket = UDPSocket.open
				socket.bind("0.0.0.0", 49452)
				socket.setsockopt(Socket::IPPROTO_IP, Socket::IP_MULTICAST_TTL, 32)
				socket.send(data, 0,"225.4.5.6", 49452)
				socket.close 
				puts Time.now.to_s + data
			rescue Exception => e
				@log.error e.to_s + "Broadcasting to client."
			end

		}

		loop { timers.wait }

		# EventMachine.run do
		#   timer = EventMachine::PeriodicTimer.new(1) do
		#   	# timer.interval = @connect_time
		#   		# puts running_thread_count
		# 		begin
		# 			# status = 0
		# 			# if !@connected.empty?
		# 			# 	status = 1
		# 			# end
		# 			# data = "tnx|#{@connected.to_s}|#{status}|#{@connect_time}"
		# 			# socket = UDPSocket.open
		# 			# socket.bind("0.0.0.0", 49452)
		# 			# socket.setsockopt(Socket::IPPROTO_IP, Socket::IP_MULTICAST_TTL, 32)
		# 			# socket.send(data, 0,"225.4.5.6", 49452)
		# 			# socket.close 
		# 			puts Time.now.to_s
		# 		rescue Exception => e
		# 			@log.error e.to_s + " - Broadcasting to client."
		# 		end
		#   end
		# end
	end

	def send1020
		if @config['1020_server']['send']
			
		end
	end

	def running_thread_count
		return Thread.list.select { |thread| thread.status == "run"}.count
	end

	def get_attribute(xml, tagname, attribute)
		XPath.first(xml, "//#{tagname}/@#{attribute}")
	end

end

Dispatch.new