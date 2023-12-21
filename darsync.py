#!/usr/bin/env python3

import argparse
import os
import pdb
import gzip

# Define a list of file extensions that are considered 'bad'
BAD_FILE_EXTENSIONS = [".sam", ".vcf", ".fq", ".fastq", ".fasta", ".txt", ".fa"]  # Add your own bad file extensions


def human_readable_size(size, units=('bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')):
    """ Returns a human readable string representation of bytes """
    return "{0:.1f} {1}".format(size, units[0]) if size < 1024 else human_readable_size(size / 1024, units[1:])


def check_file_tree(args):
    """ Traverse a directory tree and check for files with 'bad' extensions """
    local_dir     = args.local_directory
    if args.prefix:
        prefix = args.prefix
    else:
        prefix = f"darsync_{os.path.basename(os.path.abspath(local_dir))}"

    # Initialize variables for tracking file counts and sizes
    total_size       = 0
    bad_files        = []
    total_files      = 0
    crowded_dirs     = []
    large_count      = 0
    bad_count        = 0
    size_limit       = 2 * 1024 ** 3 # Size limit (2GB)
    files_limit      = 1000000 #   1M
    dir_files_limit  =  100000 # 100K

    # open ownership file
    with gzip.open(f"{prefix}.ownership.gz", 'wb') as ownership_file:

        # Walk the directory tree
        for dirpath, dirnames, filenames in os.walk(local_dir):

            # init
            dir_file_counter    = 0
            file_ownership_info = []

            for file in filenames:

                # count file and get file info
                dir_file_counter += 1
                total_files      += 1
                full_path = os.path.join(dirpath, file)
                file_info = os.stat(full_path, follow_symlinks=False)

                # Check if file has a 'bad' extension
                if any(file.endswith(ext) for ext in BAD_FILE_EXTENSIONS):
                    # Update counters and size totals
                    if file_info.st_size > size_limit:
                        large_count += 1
                    bad_count       += 1
                    total_size += file_info.st_size
                    bad_files.append((full_path, file_info.st_size))

                # add file ownership info
                file_ownership_info.append(f"{file_info.st_uid}\t{file_info.st_gid}\t{full_path}")

            # check if the dir is too crowded
            if dir_file_counter > dir_files_limit:
                crowded_dirs.append((os.path.abspath(dirpath), dir_file_counter))

            # print dirs ownership into and reset
            for line in file_ownership_info:
                ownership_file.write(f"{line}\n".encode('utf-8', "surrogateescape"))




    # Sort files by size
    bad_files.sort(key=lambda x: x[1], reverse=True)

    # If any large or 'bad' files found, print warning message and write logfile
    if large_count > 0 or total_size > size_limit:
        print(f"""WARNING: files with bad file extensions above the threshold detected:

{bad_count}\tfiles with bad file extension found
{large_count}\tfiles larger than {human_readable_size(size_limit)} found

If the total file size of all files with bad file extensions exceed {human_readable_size(size_limit)}
you should consider compressing them or converting them to a better file format.
Doing that will save you disk space as compressed formats are roughly 75% smaller 
than uncompressed. Your project could save up to {human_readable_size(total_size*0.75)} by doing this.
See https:// for more info about this.

Bad file extensions are common file formats that are uncompressed,
e.g. {", ".join(BAD_FILE_EXTENSIONS)}

To see a list of all files with bad file extensions found,
see the file {prefix}.uncompressed
-----------------------------------------------------------------

""")
        with open(f"{prefix}.uncompressed", 'w') as logfile:
            for file, size in bad_files:
                logfile.write(f"{human_readable_size(size)} {file}\n")




    # Sort folders by number of files
    crowded_dirs.sort(key=lambda x: x[1], reverse=True)

    # If any large or 'bad' files found, print warning message and write logfile
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
    
    local_dir     = args.local_dir or input(f'Enter directory to transfer (required): ')
    slurm_account = args.slurm_account or input(f'Enter SLURM account (required): ')
    username      = args.username or input(f'Enter remote username (required): ')
    hostname      = args.hostname or input(f'Enter remote hostname (default: {hostname_default}): ') or hostname_default
    ssh_key       = args.ssh_key  or input(f'Enter path to ssh key (default: {ssh_key_default}): ') or ssh_key_default
    remote_dir    = args.remote_dir or input(f'Enter remote path (required): ')

    outfile_default  = f"darsync_{os.path.basename(os.path.abspath(local_dir))}.slurm"
    outfile       = args.outfile or input(f'Enter name of script file to create (default: {outfile_default}): ') or outfile_default

    # Write the SLURM script
    with open(outfile, 'w') as script:
        script.write(f"""#!/bin/bash -l
#SBATCH -A {slurm_account}
#SBATCH -t 7-00:00:00
#SBATCH -p core
#SBATCH -n 1
#SBATCH -J darsync_{os.path.basename(os.path.abspath(local_dir))}

rsync -e "ssh -i {os.path.abspath(ssh_key)}" -cavzP {os.path.abspath(local_dir)} {username}@{hostname}:{remote_dir}
""")

    print(f"""
Created SLURM script: {outfile}

Run this command to submit it as a job:

sbatch {outfile}""")



# Set up argument parser and subcommands
parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

# 'check' subcommand
parser_check = subparsers.add_parser('check', description='Checks if a file tree contains bad file formats')
parser_check.add_argument('local_directory', help='Path to directory to check.')
parser_check.add_argument('-p', '--prefix', help='Path and prefix to where log files should be created.')
parser_check.set_defaults(func=check_file_tree)

# 'gen' subcommand
parser_gen = subparsers.add_parser('gen', description='Generates a SLURM script file containing a rsync command')
parser_gen.add_argument('-l', '--local-dir', help='Path to local directory to transfer.')
parser_gen.add_argument('-r', '--remote-dir', help='Path to the destination directory on the remote system.')
parser_gen.add_argument('-A', '--slurm-account', help='Which SLURM account to run the job as.')
parser_gen.add_argument('-u', '--username', help='The username at the remote system.')
parser_gen.add_argument('-H', '--hostname', help='The hostname of the remote system. (default dardel.pdc.kth.se)', default="dardel.pdc.kth.se")
parser_gen.add_argument('-s', '--ssh-key', help='Path to the private SSH key to use when logging in to the remote system.', default=f'{os.environ["HOME"]}/.ssh/id_rsa')
parser_gen.add_argument('-o', '--outfile', help='Path to the SLURM script to create.')
parser_gen.set_defaults(func=gen_slurm_script)

# Parse command line arguments
args = parser.parse_args()

# If no subcommand given, print help message and exit
if 'func' not in args:
    parser.print_help()
    exit(1)

# Call the function associated with the given subcommand
args.func(args)


