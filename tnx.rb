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

# daemonize 
#Process.daemon(true,false)
 
# write pid
#pid_file = File.dirname(__FILE__) + "#{__FILE__}.pid"
#File.open(pid_file, 'w') {|f| f.write Process.pid }

class Dispatch

	def initialize
		Thread::abort_on_exception = true

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
			begin
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
			rescue Exception => e
				@log.error e.to_s + "Listening to TNX."
			end
	    

	  }
	end

	def mine
		puts 'Mining....'
		# started_at = Time.now
		loop{
			# puts running_thread_count
			if !@queue.empty?
				started_at = Time.now
				workers = (0...4).map do
			    	Thread.new do
				      	begin
			          		begin
			          			msg = @queue.pop
			            		process_data(msg)
			          		rescue => e
			          			@log.error e.to_s + "Mining error."
			          		end
				        	
				      	rescue  => e
				      		@log.error e.to_s + "Worker error."
				      	end
			      	end
			    end
				workers.map(&:join); "ok"
				# puts "ESTIMATED TIME REMAINING IN HOURS:"
				# puts (Time.now - started_at).inspect
				# count = @queue.length
				# puts "consumed #{value}"
				# sleep(1)
				# puts "count #{count}"
			end
		}
	end

	def process_data(msg)
		xml = Document.new(msg)
		xmldoc = XPath.match(xml, "Tig/Call.Resource")
		if !xmldoc.empty?
			if get_attribute(xml, "Call.Resource","Status").to_s == "Granted"
				begin
					client = Mysql2::Client.new(
					  :host => @config['database']['host'], 
					  :username => @config['database']['username'],
					  :password => @config['database']['password'],
					  :database => @config['database']['database']
					)

					number = get_attribute(xml, "Tetra","Ssi")
					result = client.query("SELECT train_id,train_radios.id,head
						FROM train_radios
						INNER JOIN radios on train_radios.radio_id = radios.id
						WHERE ssi = #{number}");

						if result.count > 0
							result.each do |row|
								train_id = row["train_id"].to_s
								id = row["id"].to_s
								if row["head"] == 0
									client.query("UPDATE train_radios SET head = 0 WHERE train_id = '#{train_id}'")
									client.query("UPDATE train_radios SET head = 1 WHERE id = '#{id}'")
								end
							end
						end
					client.close
				rescue  Exception => e
					@log.error e.to_s + " MySql Update Call Error!"
				end
			end
		end

		# check if a data is sent from tnx
		xmldoc2 = XPath.match(xml, "Tig/Subscriber.Location")
		if !xmldoc2.empty?
			begin
				client = Mysql2::Client.new(
						:host => @config['database']['host'], 
						:username => @config['database']['username'],
						:password => @config['database']['password'],
						:database => @config['database']['database']
					)

				mcc = get_attribute(xml, "Tetra","Mcc")
				mnc = get_attribute(xml, "Tetra","Mnc")
				ssi = get_attribute(xml, "Tetra","Ssi")
				name = get_attribute(xml, "Name","Name")
				uplink = get_attribute(xml, "Uplink","Rssi")
				speed = 0
				if(!get_attribute(xml, "PositionFix","Speed").nil?)
					speed = get_attribute(xml, "PositionFix","Speed")
				end

				course = get_attribute(xml, "PositionFix","Course")
				alt = get_attribute(xml, "PositionFix","Altitude")
				error = get_attribute(xml, "PositionFix","MaximumPositionError")

				lat = convertDegreeAngleToDouble(get_attribute(xml, "Latitude","Degrees"),get_attribute(xml, "Latitude","Minutes"),get_attribute(xml, "Latitude","Seconds"))

				lng = convertDegreeAngleToDouble(get_attribute(xml, "Longitude","Degrees"),get_attribute(xml, "Longitude","Minutes"),get_attribute(xml, "Longitude","Seconds"))

				# check if radio exist and active
				query = "SELECT * FROM radios
					WHERE id NOT IN (SELECT radio_id FROM train_radios )
					AND radios.active = 1
					AND mcc = #{mcc}
					AND mnc = #{mnc}
					AND ssi = #{ssi}"

				send_log = false
				id = ''
				code = ''
				radio_result = client.query(query);
				if radio_result.count > 0 
					send_log = true
					radio_result.each do |row|
							radio_id = row["id"].to_s
							mcc = row["mcc"].to_s
							mnc = row["mnc"].to_s
							ssi = row["ssi"].to_s
							id = ssi
							tracker_code = row["tracker_code"].to_s
							code = tracker_code
							image_index = row["image_index"].to_s
							client.query("INSERT INTO radio_logs (radio_id, mcc, mnc, ssi, tracker_code,
								subscriber_name, uplink, speed, course, alt, max_pos_error, lat, lng, image_index)
							   VALUES ('#{radio_id}', '#{mcc}', '#{mnc}', '#{ssi}', '#{tracker_code}',
								'#{name}', '#{uplink}', '#{speed}', '#{course}', '#{alt}', '#{error}', '#{lat}', '#{lng}', '#{image_index}')")
						end
				else
					query2 = "SELECT trains.id,head,trains.train_code,trains.train_desc,mcc,mnc,ssi,tracker_code, trains.image_index
					FROM tracker.train_radios
					INNER JOIN radios on train_radios.radio_id = radios.id
					INNER JOIN trains on train_radios.train_id = trains.id
					WHERE mcc = #{mcc}
					AND mnc = #{mnc}
					AND ssi = #{ssi}"
					
					result = client.query(query2)
					if result.count > 0
						send_log = true
						result.each do |row|
							train_id = row["id"].to_s
							train_code = row["train_code"].to_s
							train_desc = row["train_desc"].to_s
							mcc = row["mcc"].to_s
							mnc = row["mnc"].to_s
							ssi = row["ssi"].to_s
							id = ssi
							tracker_code = row["tracker_code"].to_s
							code = tracker_code
							image_index = row["image_index"].to_s
							head = row["head"].to_s
							client.query("INSERT INTO logs (train_id,train_code, train_desc, mcc, mnc, ssi, tracker_code, head,
								subscriber_name, uplink, speed, course, alt, max_pos_error, lat, lng, image_index)
							   VALUES ('#{train_id}','#{train_code}', '#{train_desc}', '#{mcc}', '#{mnc}', '#{ssi}', '#{tracker_code}', '#{head}',
								'#{name}', '#{uplink}', '#{speed}', '#{course}', '#{alt}', '#{error}', '#{lat}', '#{lng}', '#{image_index}')")
						end
					end
				end	

				

				client.close

				if send_log
					if @send
				 		send1020(id,code,lat,lng)
				 	end
				end
			rescue  Exception => e
				@log.error e.to_s + " MySql Train/Radio data Error!"
				#puts response.to_s + "MySql Server cannot be found!"
			end
		end
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
				socket = UDPSocket.new
				# socket.bind("0.0.0.0", 49452)
				socket.setsockopt(Socket::IPPROTO_IP, Socket::IP_MULTICAST_TTL, 32)
				socket.send(data, 0,"225.4.5.6", 49452)
				socket.close 
				# puts Time.now.to_s + data
			rescue Exception => e
				@log.error e.to_s + "Broadcasting to client."
			end

		}

		loop { timers.wait }
	end

	def send1020(id,code,lat,lng)
		if @config['1020_server']['send']
			uri = URI(@config['1020_server']['url'])
	 		today = Time.now.strftime("%Y%m%d%H%M%S").to_i
	 		param = id.to_s+','+code.to_s+','+today.to_s+','+lat.to_s+','+lng.to_s
			res = Net::HTTP.post_form(uri, 'cmd' => 'loc', 'param' => param)
		end
	end

	def running_thread_count
		return Thread.list.select { |thread| thread.status == "run"}.count
	end

	def get_attribute(xml, tagname, attribute)
		XPath.first(xml, "//#{tagname}/@#{attribute}")
	end

	def convertDegreeAngleToDouble(degrees,minutes,seconds)
		degrees.to_s.to_f + (minutes.to_s.to_f / 60) + (seconds.to_s.to_f / 3600)
	end

end

Dispatch.new