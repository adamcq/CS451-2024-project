import argparse
import os
import math

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
  for sender_output in sender_outputs:
    with open(sender_output, 'r') as file:
      seen_messages = set()
      line_number = 0
      min_message, max_message = math.inf, 0
      sender_id = os.path.basename(sender_output).split('.')[0]
      errors = []
      delivery_errors = []
      for line in file:
        line_number += 1
        parts = line.split()
        if len(parts) == 2 and parts[0] == 'b':
          message_id = int(parts[1])
          max_message = max(max_message, message_id)
          min_message = min(min_message, message_id)
          if message_id in seen_messages:
            errors.append((message_id, line_number))
          else:
            seen_messages.add(message_id)
        if len(parts) == 3 and parts[0] == 'd':
          sender, message = int(parts[1]), int(parts[2])
          if sender not in delivered:
            delivered[sender] = set()
          if message in delivered[sender]:
            delivery_errors.append(f"ERROR: Duplicate DELIVERY found: process_id={sender_id} sender={sender}, message={message}, line_number={line_number}")
          delivered[sender].add(message)
      if errors:
        for error_message_id, error_line_number in errors:
          print(f"ERROR: Duplicate message found: sender_id={sender_id}, message_id={error_message_id}, line_number={error_line_number}")
        print(f'sender {sender_id} incorrectly broadcast {line_number - len(delivered[sender])} messages. min: {min_message} max: {max_message}')
      else:
        if sender_id in delivered: # TODO all sender_id should be in delivered!!!
          print(f'sender {sender_id} correctly broadcast {line_number - len(delivered[sender])} messages. min: {min_message} max: {max_message}')
  print()

  print("FILES DELIVERY CHECK:")
  print("Below is the number of messages delivered by each process: (should be equal for all correct processes)")
  for process_id in delivered:
    print(f'|{process_id}: {len(delivered[process_id])}', end="| ")
  print()
  print()

if __name__ == "__main__":
  main()