import argparse
import os
import math
import collections

def main():

  # parse arguments
  parser = argparse.ArgumentParser(description='Validate output script.')
  parser.add_argument('--L', required=True, help='Location of the output folder')
  parser.add_argument('--H', required=True, help='Number of hosts')
  parser.add_argument('--C', required=True, help='Config file location')

  args = parser.parse_args()

  if not args.L or not args.H or not args.C:
    parser.print_usage()
    return

  logdir = args.L
  number_of_hosts = int(args.H)
  config_file = args.C

  # Add your validation logic here
  print(f"Log directory: {logdir}")
  print(f"Number of hosts: {number_of_hosts}")
  print(f"Config file: {config_file}")
  print()

  # load the config file
  with open(config_file, 'r') as file:
    for line in file:
        num_messages = int(line)

  print("Config: num_messages:", num_messages)
  print()

  receiver_output = None
  sender_outputs = []

  # check if all output files exist
  for host_id in range(1, number_of_hosts + 1):
    # output_file = os.path.join(logdir, f"{host_id}.output")
    output_file = os.path.join(logdir, f"proc{host_id:02d}.output")
    sender_outputs.append(output_file)

  print("Receiver output file:", receiver_output)
  print("Sender output files:", sender_outputs)
  print()

  # Check if sender output files exist
  sender_outputs_to_remove = [] # TODO this should NOT be needed !!!
  for sender_output in sender_outputs:
    if not os.path.isfile(sender_output):
      sender_outputs_to_remove.append(sender_output)
      print(f"Error: Sender output file {sender_output} does not exist.")
      # return
  

  print("All output files exist.")
  print()

  # check the correctness of sender output files
  print("FILES BROADCAST CHECK:")
  delivered = {}
  for i,sender_output in enumerate(sender_outputs):
    if not os.path.isfile(sender_output): # TODO this should NOT be necessary !!!
      print(f"Error: Sender output file {sender_output} does not exist.")
      continue
    with open(sender_output, 'r') as file:
      broadcast_messages = collections.deque([])
      line_number = 0
      min_message, max_message = math.inf, 0
      sender_id = i+1 #os.path.basename(sender_output).split('.')[0]
      delivered[sender_id] = {}
      for i in range(len(sender_outputs)):
        delivered[sender_id][i+1] = collections.deque([])
      broadcast_errors = []
      delivery_errors = []

      for line in file:
        line_number += 1
        parts = line.split()

        # process broadcast line
        if len(parts) == 2 and parts[0] == 'b':
          message_id = int(parts[1])
          max_message = max(max_message, message_id)
          min_message = min(min_message, message_id)
          if message_id == 1:
            if len(broadcast_messages) > 0:
              broadcast_errors.append((message_id, line_number))
          elif message_id != broadcast_messages[-1] + 1:
            broadcast_errors.append((message_id, line_number))
          broadcast_messages.append(message_id)

        # process delivery line
        elif len(parts) == 3 and parts[0] == 'd':
          broadcaster, message = int(parts[1]), int(parts[2])
          if len(delivered[sender_id][broadcaster]) == 0:
            if message != 1:
              delivery_errors.append(f"ERROR: First message delivered to {sender_id} from {broadcaster} is {message}")
          else:
            if message != delivered[sender_id][broadcaster][-1] + 1:
              delivery_errors.append(f"ERROR: Message {message} delivered by {sender_id} from {broadcaster} is delivered at the wrong time. Line {line_number}")
          delivered[sender_id][broadcaster].append(message)
         
        # process error line
        else:
          print(f"ERROR: Line {line_number} has {len(parts)} elements")

      # print broadcast errors
      for error_message_id, error_line_number in broadcast_errors:
        print(f"ERROR: Duplicate message found: sender_id={sender_id}, message_id={error_message_id}, line_number={error_line_number}")
      
      # print delivery errors
      for delivery_error in delivery_errors:
        print(delivery_error)
      
      # print broadcast summary
      if broadcast_errors:
        print(f'BROADCAST ERROR: sender {sender_id} incorrectly broadcast {len(broadcast_messages)} messages. min: {min_message} max: {max_message}')
      else:
        print(f'BROADCAST SUCCESS: sender {sender_id} correctly broadcast {len(broadcast_messages)} messages. min: {min_message} max: {max_message}')

  # print delivery summary
  print("DELIVERY SUMMARY: Below is the number of messages delivered from each process: (should be EVENTUALLY equal for all correct processes)")
  for process_id in delivered:
    delivered_messages = 0
    for broadcaster in delivered[process_id]:
      delivered_messages += len(delivered[process_id][broadcaster])
    print(f'|{process_id}: {delivered_messages}', end="| ")
  print()


  print()

if __name__ == "__main__":
  main()