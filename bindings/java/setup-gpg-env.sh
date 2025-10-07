#!/bin/bash
# Source this script to set environment variables for Maven Central publishing
# Usage: source setup-gpg-env.sh

# Set Maven Central credentials (update these with your actual credentials)
# Get these from https://central.sonatype.com (Account -> View Account)
export MAVEN_CENTRAL_USERNAME="Y39dNM"
export MAVEN_CENTRAL_PASSWORD="SRyYCQi58oH8dS5L02g9A7AqDCzd2PC6V"

echo "✓ Environment variables set successfully!"
echo ""
echo "GPG signing will use your system's GPG installation."
echo "Make sure your GPG key is properly configured."
echo ""
echo "You can now run:"
echo "  ./gradlew publishToMavenLocal      # Publish to local Maven repository"
echo "  ./gradlew publish                  # Build and sign artifacts"
echo "  ./gradlew createMavenCentralBundle # Create bundle for Maven Central"
echo "  ./gradlew publishToMavenCentral    # Upload to Maven Central Portal"
echo ""
echo "Note: These variables are only set in your current shell session."