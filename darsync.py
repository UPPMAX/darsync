#!/usr/bin/env python3

import argparse
import os
import pdb
import gzip
import readline

# Define a list of file extensions that are considered 'uncompressed'
UNCOMPRESSED_FILE_EXTENSIONS = [".sam", ".vcf", ".fq", ".fastq", ".fasta", ".txt", ".fa"]  # Add your own uncompressed file extensions

# Enable tab completion
readline.parse_and_bind("tab: complete")

# Set the autocomplete function
def complete_path(text, state):
    # Get the current input text
    line = readline.get_line_buffer()
    # Split the line into individual arguments
    args = line.split()
    # Get the last argument (the one being completed)
    last_arg = args[-1] if len(args) > 0 else ""

    # Get the directory path and the partial file name
    dir_path, partial_name = os.path.split(last_arg)

    # Get a list of possible matches
    matches = [f for f in os.listdir(dir_path) if f.startswith(partial_name) and os.path.isdir(os.path.join(dir_path, f))]

    # If it's the first tab-completion
    if state == 0:
        # If there's only a single match
        if len(matches) == 1:
            return matches[0] + "/"
        # Otherwise, return the first match
        elif len(matches) > 0:
            return matches[0]
    # For subsequent tab-completions
    elif state < len(matches):
        # If there's only a single match
        if len(matches) == 1:
            return matches[state] + "/"
        # Otherwise, return the next match
        else:
            return matches[state]

# Set the autocomplete function for the input() function
readline.set_completer(complete_path)


def human_readable_size(size, units=('bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')):
    """ Returns a human readable string representation of bytes """
    return "{0:.1f} {1}".format(size, units[0]) if size < 1024 else human_readable_size(size / 1024, units[1:])


def check_file_tree(args):
    """ Traverse a directory tree and check for files with 'uncompressed' extensions """

    local_dir = args.local_dir or input(f'Enter directory to check (required): ')
    if args.prefix:
        prefix = args.prefix
    else:
        prefix = f"darsync_{os.path.basename(os.path.abspath(local_dir))}"

    # Initialize variables for tracking file counts and sizes
    total_size       = 0
    uncompressed_files        = []
    total_files      = 0
    crowded_dirs     = []
    large_count      = 0
    uncompressed_count        = 0
    size_limit       = 2 * 1024 ** 3 # Size limit (2GB)
    files_limit      = 1000000 #   1M
    dir_files_limit  =  100000 # 100K

    # open ownership file
    with gzip.open(f"{prefix}.ownership.gz", 'wb') as ownership_file:

        # Walk the directory tree
        for dirpath, dirnames, filenames in os.walk(local_dir):

            # init
            dir_file_counter    = 0

            for file in filenames:

                # count file and get file info
                dir_file_counter += 1
                total_files      += 1
                full_path = os.path.join(dirpath, file)
                file_info = os.stat(full_path, follow_symlinks=False)

                # Check if file has a 'uncompressed' extension
                if any(file.endswith(ext) for ext in UNCOMPRESSED_FILE_EXTENSIONS):
                    # Update counters and size totals
                    if file_info.st_size > size_limit:
                        large_count += 1
                    uncompressed_count       += 1
                    total_size += file_info.st_size
                    uncompressed_files.append((full_path, file_info.st_size))

                # add file ownership info
                ownership_file.write(f"{file_info.st_uid}\t{file_info.st_gid}\t{full_path}\n".encode('utf-8', "surrogateescape"))

            # check if the dir is too crowded
            if dir_file_counter > dir_files_limit:
                crowded_dirs.append((os.path.abspath(dirpath), dir_file_counter))




    # Sort files by size
    uncompressed_files.sort(key=lambda x: x[1], reverse=True)

    # If any large or 'uncompressed' files found, print warning message and write logfile
    if large_count > 0 or total_size > size_limit:
        print(f"""WARNING: files with uncompressed file extensions above the threshold detected:

{uncompressed_count}\tfiles with uncompressed file extension found
{large_count}\tfiles larger than {human_readable_size(size_limit)} found

If the total file size of all files with uncompressed file extensions exceed {human_readable_size(size_limit)}
you should consider compressing them or converting them to a better file format.
Doing that will save you disk space as compressed formats are roughly 75% smaller 
than uncompressed. Your project could save up to {human_readable_size(total_size*0.75)} by doing this.
See https:// for more info about this.

Uncompressed file extensions are common file formats that are uncompressed,
e.g. {", ".join(UNCOMPRESSED_FILE_EXTENSIONS)}

To see a list of all files with uncompressed file extensions found,
see the file {prefix}.uncompressed
-----------------------------------------------------------------

""")
        with open(f"{prefix}.uncompressed", 'w') as logfile:
            for file, size in uncompressed_files:
                logfile.write(f"{human_readable_size(size)} {file}\n")




    # Sort folders by number of files
    crowded_dirs.sort(key=lambda x: x[1], reverse=True)

    # If any large or 'uncompressed' files found, print warning message and write logfile
    if len(crowded_dirs) > 0 or total_files > files_limit:
        print(f"""Warning: Total number of files, or number of files in a single directory
exceeding threshold. See https:// for more info about this.

{len(crowded_dirs)}\tdirectories with more than {dir_files_limit} found
{total_files}\tfiles in total (warning threshold: {files_limit})

To see a list of all directories and the number of files they have,
see the file {prefix}.dir_n_files
-----------------------------------------------------------------

""")
        with open(f"{prefix}.dir_n_files", 'w') as logfile:
            for dir, n_files in crowded_dirs:
                logfile.write(f"{n_files} {dir}\n")

    print(f"""
Checking completed. Unless you got any warning messages above you should be good to go.

Generate a SLURM script file to do the transfer by running this script again, but use the 'gen' option this time.
See the help message for details, or continue reading the user guide for examples on how to run it.
https://

darsync gen -h

A file containing file ownership information, 
{prefix}.ownership.gz
has been created. This file can be used to make sure that the
file ownership (user/group) will look the same on Dardel as it does here. See https:// for more info about this.
""")



def gen_slurm_script(args):
    """ Generate a SLURM script for transferring files """

    # Get command line arguments, with defaults for hostname and SSH key
    hostname_default = 'dardel.pdc.kth.se'
    ssh_key_default  = f"{os.environ['HOME']}/.ssh/id_rsa"
    
    local_dir = None
    while not local_dir:
        local_dir = args.local_dir or input(f'Enter directory to transfer (required): ')
        # make sure it is a valid directory
        if local_dir:
            if not os.path.isdir(local_dir):
                print(f"ERROR: not a valid directory, {local_dir}")
                local_dir = None

    slurm_account = args.slurm_account or input(f'Enter SLURM account, i.e. UPPMAX proj id (required): ')
    username      = args.username or input(f'Enter remote username (required): ')
    hostname      = args.hostname or input(f'Enter remote hostname (default: {hostname_default}): ') or hostname_default
    remote_dir    = args.remote_dir or input(f'Enter remote path (required): ')

    ssh_key = None
    while not ssh_key:
        ssh_key  = args.ssh_key  or input(f'Enter path to ssh key (default: {ssh_key_default}): ') or ssh_key_default
        # make sure the file exists
        if ssh_key:
            if not os.path.isfile(ssh_key):
                print(f"ERROR: file does not exists, {ssh_key}")
                ssh_key = None

    outfile_default  = f"darsync_{os.path.basename(os.path.abspath(local_dir))}.slurm"
    outfile          = args.outfile or input(f'Enter name of script file to create (default: {outfile_default}): ') or outfile_default

    # Write the SLURM script
    with open(outfile, 'w') as script:
        script.write(f"""#!/bin/bash -l
#SBATCH -A {slurm_account}
#SBATCH -M snowy
#SBATCH -t 30-00:00:00
#SBATCH -p core
#SBATCH -n 1
#SBATCH -J darsync_{os.path.basename(os.path.abspath(local_dir))}

rsync -e "ssh -i {os.path.abspath(ssh_key)}" -cavzP {os.path.abspath(local_dir)} {username}@{hostname}:{remote_dir}
""")

    print(f"""
Created SLURM script: {outfile}

To test if the generated file works, run

bash {outfile}

If the transfer starts you know the script is working, and you can terminate it by pressing ctrl+c and submit the script as a SLURM job.

Run this command to submit it as a job:

sbatch {outfile}""")



# Set up argument parser and subcommands
parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

# 'check' subcommand
parser_check = subparsers.add_parser('check', description='Checks if a file tree contains uncompressed file formats or too many files.')
parser_check.add_argument('-l', '--local-dir', help='Path to directory to check.')
parser_check.add_argument('-p', '--prefix', help='Path and prefix to where log files should be created. (default: ./darsync_)')
parser_check.set_defaults(func=check_file_tree)

# 'gen' subcommand
parser_gen = subparsers.add_parser('gen', description='Generates a SLURM script file containing a rsync command')
parser_gen.add_argument('-l', '--local-dir', help='Path to local directory to transfer.')
parser_gen.add_argument('-r', '--remote-dir', help='Path to the destination directory on the remote system.')
parser_gen.add_argument('-A', '--slurm-account', help='Which SLURM account to run the job as (UPPMAX proj id).')
parser_gen.add_argument('-u', '--username', help='The username at the remote system.')
parser_gen.add_argument('-H', '--hostname', help='The hostname of the remote system. (default dardel.pdc.kth.se)', default="dardel.pdc.kth.se")
parser_gen.add_argument('-s', '--ssh-key', help='Path to the private SSH key to use when logging in to the remote system.')
parser_gen.add_argument('-o', '--outfile', help='Path to the SLURM script to create.')
parser_gen.set_defaults(func=gen_slurm_script)

# Parse command line arguments
args = parser.parse_args()


# ask interactivly if no subcommand was sent
if 'func' not in args:
    # If no subcommand given, print help message and exit
    func = input(f"""

  ____    _    ____  ______   ___   _  ____ 
 |  _ \  / \  |  _ \/ ___\ \ / / \ | |/ ___|
 | | | |/ _ \ | |_) \___ \\\ V /|  \| | |    
 | |_| / ___ \|  _ < ___) || | | |\  | |___ 
 |____/_/   \_\_| \_\____/ |_| |_| \_|\____|


Welcome to the Dardel data transfer tool. Please run `darsync -h` to see details on how to run the script using commandline options instead of interactive questions.
    
This tool can do two things;
    1) analyze a folder and make suggestions what could be done before transfering the data
    2) generate a SLURM script that you can submit to the queue that will run the data transfer.

We recommend that you run the `check` part first and fix any problems it finds, e.g. compressing files and/or removing files. Once that is done you can run this script again and choose `gen` to create a SLURM script that you submit to the queue system to do the actual data transfer.
    
You now have to choose which of these two things you want to do. Type `check` (without the quotes) to start the analysis mode, or type `gen` (without the quotes) to generate the SLURM script.
    
check/gen? : """)

    # ask for subcommand until a valid one is given
    run = True
    while run:
        if func == 'gen':
            run = False
            # init a argparse namespace to get the correct defaults etc
            subcommand_namespace = parser_gen.parse_args()
            args.__dict__.update(subcommand_namespace.__dict__)

            # call the generate function
            gen_slurm_script(args)
        elif func == 'check':
            run = False
            # init a argparse namespace to get the correct defaults etc
            subcommand_namespace = parser_check.parse_args()
            args.__dict__.update(subcommand_namespace.__dict__)

            # call the generate function
            check_file_tree(args)
        else:
            func = input(f"""
Invalid choice.
check/gen? : """)

else:
    # Call the function associated with the given subcommand
    args.func(args)


