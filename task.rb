# Programos darbo laikas su 1 darbuotoju - 17.165109399938956s
# Programos darbo laikas su 6 darbuotojais (6 branduoliai) - 3.750654999865219s

# Spausdinimo į konsolę eilutės paliktos tam, kad dėstytojui būtų
# lengviau įsigilinti, kaip veikia programa

require "json"
require "benchmark"

puts "[INFO] Program started\n"

def print_to_table(data)
    puts "[INFO] Printing to file..."
    
    File.open('IFF03_KirklysJ_E_rez.txt', 'w') do |file|
        if data.length > 0
            headers = ["ID", "First name", "Hourly rate", "Computed value"]
            column_widths = [10, 15, 15, 15]

            file.puts headers.zip(column_widths).map { |cell, width| cell.ljust(width) }.join(' ')
            file.puts headers.zip(column_widths).map { |_, width| '-' * width }.join(' ')
            for line in data do
                file.puts line.values.zip(column_widths).map { |cell, width| cell.to_s.ljust(width) }.join(' ')
            end
        else
            file.puts "Results array is empty"
        end
    end

    puts "[INFO] Data written to file"
end

# This function inserts data to array and keeps it sorted
# It is sorting data by ID
def insert_to_array(array, data)
    id = data["id"]

    # Find the index where data should be inserted
    index = 0
    while index < array.length && array[index]["id"] < id
        index += 1
    end

    # Shift all elements until found index to the right
    (array.length - 1).downto(index) do |i|
        array[i + 1] = array[i]
    end

    array[index] = data
end

num_of_ractors = 6

# Get data from file
file = File.read('./IFF03_KirklysJ_E_dat_1.json')
data_map = JSON.parse(file)

# Wrap code in benchmark
benchmark = Benchmark.measure do
    
    # Creating workers
    workers = []
    num_of_ractors.times do |i|
        workers << Ractor.new do
            distributor = Ractor.receive

            loop do
                message = Ractor.receive
                puts "Worker received #{message}\n"

                if message == -1
                    distributor.send({actor: "worker", data: -1})
                    break
                end

                computed_value = message["first_name"]
                10000000.times do
                    computed_value = computed_value.hash
                end

                if computed_value > 0
                    puts "Computed value was valid. Sending to distributor\n"
                    message["computed_value"] = computed_value
                    hash = {actor: "buffer", data: message}
                    distributor.send(hash)
                elsif
                    puts "Computed value was invalid. Skipping...\n"
                end
            end
        end
    end

    # Creating buffer for results
    buffer = Ractor.new do
        puts "[INFO] Created Buffer\n"
        distributor = Ractor.receive
        results = []

        loop do
            message = Ractor.receive

            puts "Buffer received #{message}\n"
            
            if message == -1
                puts "[INFO] Buffer finished\n"
                distributor.send({actor: "printer", data: results})
                distributor.send({actor: "buffer", data: -1})
                break
            end

            insert_to_array(results, message)
        end
    end

    # Creating printer
    printer = Ractor.new do
        puts "[INFO] Created Printer\n"
        distributor = Ractor.receive
        message = Ractor.receive
        distributor.send({actor: "printer", data: "-1"})
        puts "Printer received #{message}\n"
        print_to_table(message)
        puts "[INFO] Printer finished\n"
    end

    # Creating distributor
    distributor = Ractor.new(workers, buffer, printer) do |workers, buffer, printer| 
        puts "[INFO] Created Distributor\n"

        actor_counter = 0
        workers_finished = 0
        loop do
            message = Ractor.receive
            puts "Distributor received: #{message}\n"

            # Check for the end of stream from main
            if message == -1
                puts "[INFO] End of main stream in distributor\n"
                for worker in workers do
                    worker.send(-1)
                end
                next
            end

            if message[:actor] == "worker"
                # If worker sent -1 - it finished work
                if message[:data] == -1
                    workers_finished = workers_finished + 1

                    if workers_finished == workers.length
                        buffer.send(-1)
                    end
                else
                    # Send data to workers
                    # (Assuming I don't know how much data there will be.
                    # Could use each_slice if I knew)
                    workers[actor_counter % workers.length].send(message[:data])
                    actor_counter = actor_counter + 1
                end
            elsif message[:actor] == "buffer"
                # Send filtered data to buffer
                buffer.send(message[:data])
            elsif message[:actor] == "printer"
                # If printer sent -1 - distributor (this) can finish work
                if message[:data] == -1
                    break
                end

                # Send data from buffer to printer
                printer.send(message[:data])
            end
        end
        puts "[INFO] Distributor finished\n"
    end

    # Sending each actor a reference to a distributor
    for worker in workers do
        worker.send(distributor)
    end
    buffer.send(distributor)
    printer.send(distributor)

    # Send data from file to distributor
    for person in data_map do
        hash = {actor: "worker", data: person}
        distributor.send(hash) 
    end
    distributor.send(-1)

    # Wait for actors to finish
    for worker in workers do
        worker.take
    end
    buffer.take
    distributor.take
    printer.take

# End benchmark and print results
end

puts "Elapsed time: #{benchmark.real}s\n"