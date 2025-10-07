#!/bin/bash
# Source this script to set GPG environment variables for local Maven publishing
# Usage: source setup-gpg-env.sh

# Your GPG Key ID
GPG_KEY_ID="FAF892C136A2C931"

# Export the GPG private key
echo "Exporting GPG private key..."
export GPG_PRIVATE_KEY="$(gpg --armor --export-secret-keys $GPG_KEY_ID)"

# Check if export was successful
if [ -z "$GPG_PRIVATE_KEY" ]; then
    echo "Error: Failed to export GPG private key. Make sure the key ID is correct."
    return 1 2>/dev/null || exit 1
fi

# Set the passphrase
export GPG_PASSPHRASE="test"

# Set Maven Central credentials (update these with your actual credentials)
# Get these from https://central.sonatype.com (Account -> View Account)
export MAVEN_CENTRAL_USERNAME="Y39dNM"
export MAVEN_CENTRAL_PASSWORD="SRyYCQi58oH8dS5L02g9A7AqDCzd2PC6V"

echo "✓ Environment variables set successfully!"
echo ""
echo "You can now run:"
echo "  ./gradlew publishToMavenLocal      # Publish to local Maven repository"
echo "  ./gradlew publish                  # Build and sign artifacts"
echo "  ./gradlew createMavenCentralBundle # Create bundle for Maven Central"
echo "  ./gradlew publishToMavenCentral    # Upload to Maven Central Portal"
echo ""
echo "Note: These variables are only set in your current shell session."