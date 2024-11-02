import argparse
import os
import math

def main():

  # parse arguments
  parser = argparse.ArgumentParser(description='Validate output script.')
  parser.add_argument('--L', required=True, help='Location of the output folder')
  parser.add_argument('--H', required=True, help='File with information about hosts')
  parser.add_argument('--C', required=True, help='Config file location')

  args = parser.parse_args()

  if not args.L or not args.H or not args.C:
    parser.print_usage()
    return

  logdir = args.L
  hosts_file = args.H
  config_file = args.C

  # Add your validation logic here
  print(f"Log directory: {logdir}")
  print(f"Hosts file: {hosts_file}")
  print(f"Config file: {config_file}")
  print()

  # load the host file
  hosts = {}
  with open(hosts_file, 'r') as file:
    for line in file:
      parts = line.split()
      if len(parts) == 3:
        process_id, ip, port = parts
        hosts[int(process_id)] = (ip, int(port))

  print("Hosts:", hosts)
  print()

  # load the config file
  with open(config_file, 'r') as file:
    for line in file:
      parts = line.split()
      if len(parts) == 2:
        num_messages, receiver_id = map(int, parts)

  print("Config: num_messages:", num_messages, "receiver_id:", receiver_id)
  print()

  receiver_output = None
  sender_outputs = []

  # check if all output files exist
  for host_id in hosts.keys():
    output_file = os.path.join(logdir, f"{host_id}.output")
    # output_file = os.path.join(logdir, f"proc{host_id:02d}.output")
    if host_id == receiver_id:
      receiver_output = output_file
    else:
      sender_outputs.append(output_file)

  print("Receiver output file:", receiver_output)
  print("Sender output files:", sender_outputs)
  print()

  # Check if receiver output file exists
  if not os.path.isfile(receiver_output):
    print(f"Error: Receiver output file {receiver_output} does not exist.")
    return

  # Check if sender output files exist
  for sender_output in sender_outputs:
    if not os.path.isfile(sender_output):
      print(f"Error: Sender output file {sender_output} does not exist.")
      return

  print("All output files exist.")
  print()

  # check the correctness of sender output files
  # print("SENDER FILES CHECK:")
  # for sender_output in sender_outputs:
  #   with open(sender_output, 'r') as file:
  #     seen_messages = set()
  #     line_number = 0
  #     min_message, max_message = math.inf, 0
  #     sender_id = os.path.basename(sender_output).split('.')[0]
  #     errors = []
  #     for line in file:
  #       line_number += 1
  #       parts = line.split()
  #       if len(parts) == 2 and parts[0] == 'b':
  #         message_id = int(parts[1])
  #         max_message = max(max_message, message_id)
  #         min_message = min(min_message, message_id)
  #         if message_id in seen_messages:
  #           errors.append((message_id, line_number))
  #         else:
  #           seen_messages.add(message_id)
  #     if errors:
  #       for error_message_id, error_line_number in errors:
  #         print(f"ERROR: Duplicate message found: sender_id={sender_id}, message_id={error_message_id}, line_number={error_line_number}")
  #       print(f'sender {sender_id} incorrectly logged {line_number} messages. min: {min_message} max: {max_message}')
  #     else:
  #       print(f'sender {sender_id} correctly logged {line_number} messages. min: {min_message} max: {max_message}')
  # print()

  # check correctness of delivered file
  print("RECEIVER FILE CHECK:")
  with open(receiver_output, 'r') as file:
    seen_messages = set()
    line_number = 0
    receiver_id = os.path.basename(receiver_output).split('.')[0]
    errors = []
    for line in file:
      line_number += 1
      parts = line.split()
      if len(parts) == 3 and parts[0] == 'd':
        sender_id = int(parts[1])
        message_id = int(parts[2])
        if (sender_id, message_id) in seen_messages:
          errors.append((message_id, line_number))
        else:
          seen_messages.add((sender_id, message_id))
    if errors:
      for error_message_id, error_line_number in errors:
        print(f"ERROR: Duplicate message found: sender_id={sender_id}, message_id={error_message_id}, line_number={error_line_number}")
      print(f'receiver {receiver_id} incorrectly logged {line_number} messages.')
    else:
      print(f'receiver {receiver_id} correctly logged {line_number} messages.')
  print()

if __name__ == "__main__":
  main()