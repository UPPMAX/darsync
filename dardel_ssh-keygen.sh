#!/bin/bash

# Set flags for ssh key and config changes
key_changed=false
config_changed=false

# Check if the ssh key file already exists
while [[ -f ~/.ssh/id-ed25519-pdc ]]; do
    read -p "The ssh key file already exists. Do you want to overwrite it (o), continue without changing it (c), or delete it (d)? (o/c/d): " option

    # If the user wants to overwrite the file
    if [[ $option == "o" ]]; then
        echo "Removing old ssh key."
        rm ~/.ssh/id-ed25519-pdc
        key_changed=true
        break

    # If the user wants to continue without modifying
    elif [[ $option == "c" ]]; then
        echo "Continuing without modifying the ssh key."
        break

    # If the user wants to add a password
    elif [[ $option == "d" ]]; then
        echo "Deleting the ssh key."
        rm ~/.ssh/id-ed25519-pdc
        key_deleted=true
        break
    fi
done

# Create the ssh key without a password unless it should be deleted
if [[ ! -f ~/.ssh/id-ed25519-pdc ]] && [[ $option != "d" ]]; then
    ssh-keygen -q -t ed25519 -f ~/.ssh/id-ed25519-pdc -N ""
    key_changed=true
fi

# Print final message based on changes made
if [[ $key_changed == true ]]; then
    echo "SSH key created successfully."
elif [[ $key_deleted == true ]]; then
    echo "SSH key deleted successfully."
else
    echo "No changes were made."
fi
