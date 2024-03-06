#!/usr/bin/env python3

import argparse
import os
import sys
import pdb
import gzip
import readline
import stat
import subprocess

# Define a list of file extensions that are considered 'uncompressed'
UNCOMPRESSED_FILE_EXTENSIONS = [".sam", ".vcf", ".fq", ".fastq", ".fasta", ".txt", ".fa"]  # Add your own uncompressed file extensions

def msg(id, lang='en', **kwargs):
    """ Returns a message in the specified language """
    msgs = {'en':
                {
                    "script_intro": """

  ____    _    ____  ______   ___   _  ____ 
 |  _ \  / \  |  _ \/ ___\ \ / / \ | |/ ___|
 | | | |/ _ \ | |_) \___ \\\ V /|  \| | |    
 | |_| / ___ \|  _ < ___) || | | |\  | |___ 
 |____/_/   \_\_| \_\____/ |_| |_| \_|\____|

Welcome to the Dardel data transfer tool.

Please run `darsync -h` to see details on how to run the script using command line options instead of interactive questions.

    
This tool can do two things;
    1) analyze a folder and make suggestions what could be done before transferring the data
    2) generate a SLURM script that you can submit to the queue that will run the data transfer.

We recommend that you run the `check` part first and fix any problems it finds, e.g. compressing files and/or removing files. Once that is done you can run this script again and choose `gen` to create a SLURM script that you submit to the queue system to do the actual data transfer.
    
You now have to choose which of these two things you want to do. Type `check` (without the quotes) to start the analysis mode, or type `gen` (without the quotes) to generate the SLURM script.
    
check/gen? : """,

                    "check_intro": """\n
   ____ _   _ _____ ____ _  __
  / ___| | | | ____/ ___| |/ /
 | |   | |_| |  _|| |   | ' /
 | |___|  _  | |__| |___| . \\
  \____|_| |_|_____\____|_|\_\\

The check module of this script will recursively go through 
all the files in, and under, the folder you specify to see if there 
are any improvements you can to do save space and speed up the data transfer. 

It will look for file formats that are uncompressed, like .fasta and .vcf files 
(most uncompressed file formats have compressed variants of them that only 
take up 25% of the space of the uncompressed file).

If you have many small files, e.g. folders with 100 000 or more files, 
it will slow down the data transfer since there is an overhead cost per file 
you want to transfer. Large folders like this can be archived/packed into 
a single file to speed things up.""",

                    "check_outro": """\n\n
Checking completed. Unless you got any warning messages above you should be good to go.

Generate a SLURM script file to do the transfer by running this script again, but use the 'gen' option this time.
See the help message for details, or continue reading the user guide for examples on how to run it.
https://

darsync gen -h

A file containing file ownership information, 
{prefix}.ownership.gz
has been created. This file can be used to make sure that the
file ownership (user/group) will look the same on Dardel as it does here. See https:// for more info about this.
    """,

                    "gen_intro": """\n
   ____ _____ _   _
  / ___| ____| \ | |
 | |  _|  _| |  \| |
 | |_| | |___| |\  |
  \____|_____|_| \_|

The gen module of this script will collect the information needed
and generate a script that can be submitted to SLURM to preform the
data transfer.

It will require you to know 

    1) Which directory on UPPMAX you want to transfer (local directory).
    2) Which UPPMAX project id the SLURM job should be run under. 
        ex. naiss2099-23-999
    3) Which username you have at Dardel.
    4) Where on Dardel it should transfer your data to. 
        ex. /cfs/klemming/projects/snic/naiss2099-23-999/from_uppmax
    5) Which SSH key should be used when connecting to Dardel.
        ex. /home/user/id_ed25519_pdc
    6) Where you want to save the generated SLURM script. 
    """,

                    "sshkey_intro": """\n
  ____ ____  _   _ _  _________   __
 / ___/ ___|| | | | |/ / ____\ \ / /
 \___ \___ \| |_| | ' /|  _|  \ V /
  ___) |__) |  _  | . \| |___  | |
 |____/____/|_| |_|_|\_\_____| |_|
        
The sshkey module of this script will generate a SSH key pair that you can use to login to Dardel.
It will create two files, one with the private key and one with the public key.
The private key should be kept secret and the public key should be added to your authorized_keys file on Dardel.
""",

                    "sshkey_outro": """\n\n
You will now have to add the public key above to the Dardel Login Portal, https://loginportal.pdc.kth.se

See the user guide for more info about this, http://docs.uppmax.uu.se/cluster_guides/migrate_dardel/#4-add-the-public-key-to-the-pdc-login-portal.
""",

                    "input_local_dir": """\n\nSpecify which directory you want to copy. 
Make sure to use tab completion (press the tab key to complete directory names) 
to avoid spelling errors.
Ex.
/proj/naiss2099-22-999/
or
/proj/naiss2099-22-999/raw_data_only

Specify local directory: """,

                    "input_slurm_account": """\n\nSpecify which project id should be used to run the data transfer job in SLURM.
Ex.
naiss2099-23-999

Specify project id: """,

                    "input_username": """\n\nSpecify the username that should be used to login at Dardel. It is the username you have created at PDC and it is probably not the same as your UPPMAX username.

Specify Dardel username: """,

                    "input_remote_dir": """\n\nSpecify the directory on Dardel you want to transfer your data to.
Ex.
/cfs/klemming/projects/snic/naiss2099-23-999

Specify Dardel path: """,

                    "input_ssh_key": """\n\nSpecify which SSH key should be used to login to Dardel. Create one by running `dardel_ssh-keygen` if you have not done so yet. If no path is given it will use the default key created by `dardel_ssh-keygen`, ~/id_ed25519_pdc
                    
Specify SSH key: """,

                    "input_outfile": """\n\nSpecify where the SLURM script file should be saved. If not given it will save it here: {outfile_default}
                    
Specify SLURM script path: """,

                    "uncompressed_warning": """\n\n\nWARNING: files with uncompressed file extensions above the threshold detected:

{uncompressed_count}\tfiles with uncompressed file extension found
{large_count}\tfiles larger than {human_readable_size_limit} found
{human_readable_total_size}\ttotal size of all uncompressed files

If the total file size of all files with uncompressed file extensions exceed {human_readable_size_limit}
you should consider compressing them or converting them to a better file format.
Doing that will save you disk space as compressed formats are roughly 75% smaller 
than uncompressed. Your project could save up to {human_readable_save_size} by doing this.
See https:// for more info about this.

Uncompressed file extensions are common file formats that are uncompressed,
e.g. {UNCOMPRESSED_FILE_EXTENSIONS_STR}

To see a list of all files with uncompressed file extensions found,
see the file {prefix}.uncompressed
-----------------------------------------------------------------""",
                    "too_many_files_warning": """\n\n\nWARNING: Total number of files, or number of files in a single directory
exceeding threshold. See https:// for more info about this.

{crowded_dirs_len}\tdirectories with more than {dir_files_limit} files found
{total_files}\tfiles in total (warning threshold: {files_limit})

To see a list of all directories and the number of files they have,
see the file {prefix}.dir_n_files
-----------------------------------------------------------------""",
        },

    }

    #
    return msgs[lang][id].format(**kwargs)



# Enable tab completion
readline.parse_and_bind("tab: complete")

# Change word delimiters
readline.set_completer_delims(' \t\n=/')

# Set the autocomplete function
def complete_path(text, state):
    # Get the current input text
    line = readline.get_line_buffer()
    # Expand any home folder tildes
    line = os.path.expanduser(line)
    # Split the line into individual arguments
    args = line.split()
    # Get the last argument (the one being completed)
    last_arg = args[-1] if len(args) > 0 else ""

    # Get the directory path and the partial file name
    dir_path, partial_name = os.path.split(last_arg)

    # Get a list of possible matches
    matches = [f for f in os.listdir(dir_path) if f.startswith(partial_name)] 

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


def human_readable_size(size, units=('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')):
    """ Returns a human readable string representation of bytes """
    return "{0:.1f} {1}".format(size, units[0]) if size < 1024 else human_readable_size(size / 1024, units[1:])

#pip install line_profiler
#kernprof -l darsync.py check -l /path/to/testdir
#python -m line_profiler darsync.py.lprof
#@profile
def check_file_tree(args):
    """ Traverse a directory tree and check for files with 'uncompressed' extensions 
        and directories with too many files
    """

    # print intro message
    print(msg('check_intro'))

    # Get command line arguments
    local_dir = args.local_dir or input(msg('input_local_dir'))
    while not local_dir:
        local_dir = args.local_dir or input(msg('input_local_dir'))
        # make sure it is a valid directory
        if local_dir:
            if not os.path.isdir(local_dir):
                print(f"ERROR: not a valid directory, {local_dir}")
                local_dir = None

    if args.prefix:
        prefix = args.prefix
    else:
        prefix = f"darsync_{os.path.basename(os.path.abspath(local_dir))}"

    # expand tilde
    local_dir = os.path.abspath(os.path.expanduser(local_dir))
    prefix = os.path.abspath(os.path.expanduser(prefix))

    # Initialize variables for tracking file counts and sizes
    total_size           = 0
    uncompressed_files   = []
    total_files          = 0
    crowded_dirs         = []
    large_count          = 0
    uncompressed_count   = 0
    size_limit           = 2 * 1024 ** 3 # Size limit (2GB)
    files_limit          = 1000000 #   1M
    dir_files_limit      =  100000 # 100K
    previous_dirpath_len = 0

    # open ownership file
    with gzip.open(f"{prefix}.ownership.gz", 'wb') as ownership_file:

        # Walk the directory tree
        for dirpath, dirnames, filenames in os.walk(local_dir):

            # print progress
            print(f"\r{dirpath}" + ' '*(previous_dirpath_len-len(dirpath)), end='')
            previous_dirpath_len = len(dirpath)

            # init
            dir_file_counter    = 0

            # save directory permissions
            file_info = os.lstat(dirpath)
            ownership_file.write(f"{stat.S_IMODE(file_info.st_mode)}\t{file_info.st_uid}\t{file_info.st_gid}\t{dirpath}/\n".encode('utf-8', "surrogateescape"))

            # Loop over files in directory
            for file in filenames:

                # count file and get file info
                dir_file_counter += 1
                total_files      += 1
                full_path = os.path.join(dirpath, file)
                file_info = os.lstat(full_path)

                # Check if file has a 'uncompressed' extension
                if any(file.endswith(ext) for ext in UNCOMPRESSED_FILE_EXTENSIONS):
                    # Update counters and size totals
                    if file_info.st_size > size_limit:
                        large_count    += 1
                    uncompressed_count += 1
                    total_size += file_info.st_size
                    uncompressed_files.append((full_path, file_info.st_size))

                # add file ownership info
                ownership_file.write(f"{stat.S_IMODE(file_info.st_mode)}\t{file_info.st_uid}\t{file_info.st_gid}\t{full_path}\n".encode('utf-8', "surrogateescape"))


            # check if the dir is too crowded
            if dir_file_counter > dir_files_limit:
                crowded_dirs.append((os.path.abspath(dirpath), dir_file_counter))




    # Sort files by size
    uncompressed_files.sort(key=lambda x: x[1], reverse=True)

    # If any large or 'uncompressed' files found, print warning message and write logfile
    if large_count > 0 or total_size > size_limit or args.devel:
        print(msg('uncompressed_warning', uncompressed_count=uncompressed_count, large_count=large_count, human_readable_size_limit=human_readable_size(size_limit), human_readable_save_size=human_readable_size(total_size*0.75), UNCOMPRESSED_FILE_EXTENSIONS_STR=", ".join(UNCOMPRESSED_FILE_EXTENSIONS), prefix=prefix, human_readable_total_size=human_readable_size(total_size)))
        with open(f"{prefix}.uncompressed", 'w') as logfile:
            for file, size in uncompressed_files:
                logfile.write(f"{human_readable_size(size)} {file}\n")




    # Sort folders by number of files
    crowded_dirs.sort(key=lambda x: x[1], reverse=True)

    # If any large or 'uncompressed' files found, print warning message and write logfile
    if len(crowded_dirs) > 0 or total_files > files_limit or args.devel:
        print(msg("too_many_files_warning", crowded_dirs_len=len(crowded_dirs), dir_files_limit=dir_files_limit, total_files=total_files, files_limit=files_limit, prefix=prefix))
        with open(f"{prefix}.dir_n_files", 'w') as logfile:
            for dir, n_files in crowded_dirs:
                logfile.write(f"{n_files} {dir}\n")

    print(msg('check_outro', prefix=prefix))



def gen_slurm_script(args):
    """ Generate a SLURM script for transferring files """

    # print intro message
    print(msg('gen_intro'))

    # Get command line arguments, with defaults for hostname and SSH key
    hostname_default = 'dardel.pdc.kth.se'
    ssh_key_default  = f"{os.environ['HOME']}/id_ed25519_pdc"

    # Get command line arguments
    local_dir = args.local_dir or input(msg('input_local_dir'))
    while not local_dir:
        local_dir = args.local_dir or input(msg('input_local_dir'))
        # make sure it is a valid directory
        if local_dir:
            local_dir = os.path.expanduser(local_dir)
            if not os.path.isdir(local_dir):
                print(f"ERROR: not a valid directory, {local_dir}")
                local_dir = None

    slurm_account = None
    while not slurm_account:
        slurm_account = args.slurm_account or input(msg("input_slurm_account"))

    username = None
    while not username:
        username = args.username or input(msg("input_username"))

    # don't ask for a hostname, but leave it open to specify another one on commandline
    hostname      = args.hostname or hostname_default

    remote_dir = None
    while not remote_dir:
        remote_dir = args.remote_dir or input(msg("input_remote_dir"))

    ssh_key = None
    while not ssh_key:
        ssh_key  = args.ssh_key  or input(msg("input_ssh_key")) or ssh_key_default
        # make sure the file exists
        if ssh_key:
            ssh_key = os.path.expanduser(ssh_key)
            if not os.path.isfile(ssh_key):
                print(f"ERROR: file does not exists, {ssh_key}")
                ssh_key = None

    outfile_default  = f"darsync_{os.path.basename(os.path.abspath(local_dir))}.slurm"

    outfile = args.outfile or input(msg("input_outfile", outfile_default=outfile_default)) or outfile_default
    outfile = os.path.abspath(os.path.expanduser(outfile))

    if args.dryrun:
        print(f"""



Dry run.
Would have created SLURM script: {outfile}

containing the this:

#!/bin/bash -l
#SBATCH -A {slurm_account}
#SBATCH -M snowy
#SBATCH -t 10-00:00:00
#SBATCH -p core
#SBATCH -n 1
#SBATCH -J darsync_{os.path.basename(os.path.abspath(local_dir))}

rsync -e "ssh -i {os.path.abspath(ssh_key)}" -acPuv {os.path.abspath(local_dir)}/ {username}@{hostname}:{remote_dir}

""")

    else:
        # Write the SLURM script
        with open(outfile, 'w') as script:
            script.write(f"""#!/bin/bash -l
#SBATCH -A {slurm_account}
#SBATCH -M snowy
#SBATCH -t 10-00:00:00
#SBATCH -p core
#SBATCH -n 1
#SBATCH -J darsync_{os.path.basename(os.path.abspath(local_dir))}

rsync -e "ssh -i {os.path.abspath(ssh_key)}" -acPuv {os.path.abspath(local_dir)}/ {username}@{hostname}:{remote_dir}
""")

        print(f"""



Created SLURM script: {outfile}

containing the following command:

rsync -e "ssh -i {os.path.abspath(ssh_key)}" -acPuvz {os.path.abspath(local_dir)}/ {username}@{hostname}:{remote_dir}


To test if the generated file works, run

bash {outfile}

If the transfer starts you know the script is working, and you can terminate it by pressing ctrl+c and submit the script as a SLURM job.

Run this command to submit it as a job:

sbatch {outfile}""")





def create_ssh_keys(args):
    """ Generate ssh keys for the user """

    # print intro message
    print(msg('sshkey_intro'))

    # Get command line arguments
    output = args.output or f"{os.environ['HOME']}/id_ed25519_pdc"

    # generate the key
    process = subprocess.run(f"yes | ssh-keygen -q -N '' -t ed25519 -f {output}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = process.stdout
    stderr = process.stderr
    print(stdout.decode('utf-8'))
    print("\n", stderr.decode('utf-8'))

    # make sure the file exists
    if not os.path.isfile(output):
        print(f"ERROR: something went wrong with the SSH key creation, file does not exist:  {output}")
        sys.exit(1)

    # print the result
    print(f"""Created SSH key: {output} and {output}.pub

Content of the public key:

{open(output + ".pub").read()}""")

    # print intro message
    print(msg('sshkey_outro'))

# Set up argument parser and subcommands
parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

# 'check' subcommand
parser_check = subparsers.add_parser('check', description='Checks if a file tree contains uncompressed file formats or too many files.')
parser_check.add_argument('-l', '--local-dir', help='Path to directory to check.')
parser_check.add_argument('-p', '--prefix', help='Path and prefix to where log files should be created. (default: ./darsync_foldername)')
parser_check.add_argument('-d', '--devel', action="store_true", help='Trigger all warnings.')
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
parser_gen.add_argument('-d', '--dryrun', action="store_true", help='Dry run, do not actually create the SLURM script.')
parser_gen.set_defaults(func=gen_slurm_script)

# 'sshkey' subcommand
parser_sshkey = subparsers.add_parser('sshkey', description='Generates a SSH key pair that can be used to login to Dardel.')
parser_sshkey.add_argument('-o', '--output', help='Path to where the key will be created. (deafult: ~/id_ed25519_pdc)')
parser_sshkey.set_defaults(func=create_ssh_keys)

# Parse command line arguments
args = parser.parse_args()


# ask interactively if no subcommand was sent
if 'func' not in args:
    # If no subcommand given, print help message and exit
    func = input(msg("script_intro"))

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
        elif func == 'sshkey':
            run = False
            # init a argparse namespace to get the correct defaults etc
            subcommand_namespace = parser_sshkey.parse_args()
            args.__dict__.update(subcommand_namespace.__dict__)
        else:
            func = input(f"""
Invalid choice.
check/gen/sshkey? : """)

else:
    # Call the function associated with the given subcommand
    args.func(args)


