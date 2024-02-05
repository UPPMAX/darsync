#!/bin/bash

# Set flags for ssh key and config changes
key_changed=false
config_changed=false

# Check if the ssh key file already exists
while [[ -f ~/.ssh/id-ed25519-pdc ]]; do
    read -p "The ssh key file already exists. Do you want to overwrite it (o), add a password to it (p), or continue without modifying (c)? (o/p/c): " option

    # If the user wants to overwrite the file
    if [[ $option == "o" ]]; then

        rm ~/.ssh/id-ed25519-pdc
        key_changed=true
        break
    # If the user wants to add a password
    elif [[ $option == "p" ]]; then
        read -s -p "Enter password for the ssh key file: " password
        ssh-keygen -p -P "" -N "$password" -f ~/.ssh/id-ed25519-pdc
        key_changed=true
        break
    # If the user wants to continue without modifying
    elif [[ $option == "c" ]]; then
        echo "Continuing without modifying the ssh key."
        break
    fi
done

# Create the ssh key without a password if it doesn't exist or was overwritten
if [[ ! -f ~/.ssh/id-ed25519-pdc ]]; then
    ssh-keygen -q -t ed25519 -f ~/.ssh/id-ed25519-pdc -N ""
    key_changed=true
fi

# Check if the config entry already exists
if grep -q "Host dardel.pdc.kth.se" ~/.ssh/config; then
    echo "Config entry for 'Host dardel.pdc.kth.se' already exists."
else
    # Append the config entry to the config file
    echo -e "\nHost dardel.pdc.kth.se\n\tIdentityFile ~/.ssh/id-ed25519-pdc" >> ~/.ssh/config
    config_changed=true
fi

# Print final message based on changes made
if [[ $key_changed == true && $config_changed == true ]]; then
    echo "SSH key created and config updated successfully."
elif [[ $key_changed == true ]]; then
    echo "SSH key created successfully."
elif [[ $config_changed == true ]]; then
    echo "Config updated successfully."
else
    echo "No changes were made."
fi
