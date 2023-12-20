import argparse
import os
import pdb

BAD_FILE_EXTENSIONS = [".sam", ".vcf", ".fq", ".fastq", ".fasta", ".txt", ".fa"]  # Add your own bad file extensions


def human_readable_size(size, units=('bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')):
    """ Returns a human readable string representation of bytes """
    return "{0:.1f} {1}".format(size, units[0]) if size < 1024 else human_readable_size(size / 1024, units[1:])


def check_file_tree(args):
    local_dir     = args.local_directory
    total_size   = 0
    bad_files    = []
    large_count  = 0
    bad_count    = 0
    size_limit   = 2 * 1024 ** 3

    for dirpath, dirnames, filenames in os.walk(local_dir):
        for file in filenames:
            if any(file.endswith(ext) for ext in BAD_FILE_EXTENSIONS):
                full_path = os.path.join(dirpath, file)
                size = os.path.getsize(full_path)

                if size > size_limit:
                    large_count += 1
                bad_count       += 1


                total_size += size
                bad_files.append((full_path, size))

    bad_files.sort(key=lambda x: x[1], reverse=True)

    if large_count > 0 or total_size > size_limit:
        print(f"""Warning: files with bad file extensions above the threshold detected:

{bad_count}\tfiles with bad file extension found
{large_count}\tfiles  larger than {human_readable_size(size_limit)} found

If the total file size of all files with bad file extensions exceed {human_readable_size(size_limit)}
you should consider compressing them or converting them to a better file format.
Doing that will save you disk space as compressed formats are roughly 75% smaller 
than uncompressed. Your project could save up to {human_readable_size(total_size*0.75)} by doing this.

Bad file extensions are common file formats that are uncompressed,
e.g. {", ".join(BAD_FILE_EXTENSIONS)}

To see a list of all files with bad file extensions found,
see the file darsync_{os.path.basename(os.path.abspath(local_dir))}.log""")
        with open(f"darsync_{os.path.basename(os.path.abspath(local_dir))}.log", 'w') as logfile:
            for file, size in bad_files:
                logfile.write(f"{human_readable_size(size)} {file}\n")


def gen_slurm_script(args):
    hostname_default = 'dardel.pdc.kth.se'
    ssh_key_default  = f"{os.environ['HOME']}/.ssh/id_rsa"
    
    local_dir     = args.local_dir or input(f'Enter directory to transfer (required): ')
    slurm_account = args.slurm_account or input(f'Enter SLURM account (required): ')
    username      = args.username or input(f'Enter remote username (required): ')
    hostname      = args.hostname or input(f'Enter remote hostname (default: {hostname_default}): ') or hostname_default
    ssh_key       = args.ssh_key  or input(f'Enter path to ssh key (default: {ssh_key_default}): ') or ssh_key_default
    remote_dir    = args.remote_dir or input(f'Enter remote path (required): ')

    with open(f"darsync_{os.path.basename(os.path.abspath(local_dir))}.slurm", 'w') as script:
        script.write(f"""#!/bin/bash -l
#SBATCH -A {slurm_account}
#SBATCH -t 7-00:00:00
#SBATCH -p core
#SBATCH -n 1
#SBATCH -J darsync_{os.path.basename(os.path.abspath(local_dir))}

rsync -e "ssh -i {os.path.abspath(ssh_key)}" -cavzP {os.path.abspath(local_dir)} {username}@{hostname}:{remote_dir}
""")


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

parser_check = subparsers.add_parser('check')
parser_check.add_argument('local_directory')
parser_check.set_defaults(func=check_file_tree)

parser_gen = subparsers.add_parser('gen')
parser_gen.add_argument('-l', '--local-dir')
parser_gen.add_argument('-r', '--remote-dir')
parser_gen.add_argument('-A', '--slurm-account')
parser_gen.add_argument('-u', '--username')
parser_gen.add_argument('-H', '--hostname')
parser_gen.add_argument('-s', '--ssh-key')
parser_gen.set_defaults(func=gen_slurm_script)

args = parser.parse_args()
args.func(args)


