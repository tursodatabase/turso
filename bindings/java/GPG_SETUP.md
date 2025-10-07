# GPG Setup for Maven Central Publishing

This guide walks you through setting up GPG keys for publishing to Maven Central, following the [official requirements](https://central.sonatype.org/publish/requirements/gpg/).

## Prerequisites

- Install GPG:
  - **macOS**: `brew install gnupg`
  - **Linux**: `sudo apt-get install gnupg` or `sudo yum install gnupg`
  - **Windows**: Download from https://www.gnupg.org/download/

## Step 1: Generate a New GPG Key Pair

```bash
# Generate a new key (use default options)
gpg --gen-key
```

**When prompted:**
1. **Name**: Your real name (e.g., `Seon Woo Kim`)
2. **Email**: Your email address (e.g., `seonwoo960000.kim@gmail.com`)
3. **Passphrase**: Choose a strong passphrase (save it securely!)

**Important Notes:**
- Maven Central requires RSA keys with at least 2048 bits
- Keys should have a validity period (default is 2 years - this is fine)
- Use a passphrase you'll remember but is hard to guess

## Step 2: List Your Keys

```bash
# List your secret keys
gpg --list-secret-keys --keyid-format=long
```

**Example output:**
```
sec   rsa3072/FAF892C136A2C931 2025-10-07 [SC] [expires: 2028-10-06]
      EAEF09EBB451F696FE24CF64FAF892C136A2C931
uid                 [ultimate] Seon Woo Kim <seonwoo960000.kim@gmail.com>
ssb   cv25519/B7F51EDB88321B47 2025-10-07 [E] [expires: 2028-10-06]
```

**Note your Key ID**: The part after the `/` (e.g., `FAF892C136A2C931`)

## Step 3: Export Your Public Key

```bash
# Replace with your actual key ID
export GPG_KEY_ID="FAF892C136A2C931"

# Export to a file
gpg --armor --export $GPG_KEY_ID > public-key.asc

# Display the key (for copy-paste)
gpg --armor --export $GPG_KEY_ID
```

## Step 4: Upload to Keyservers

Maven Central checks multiple keyservers. Upload to at least 2-3 of them:

### Option A: Command Line Upload

```bash
# Upload to keys.openpgp.org (recommended)
gpg --keyserver hkps://keys.openpgp.org --send-keys $GPG_KEY_ID

# Upload to Ubuntu keyserver
gpg --keyserver hkps://keyserver.ubuntu.com --send-keys $GPG_KEY_ID

# Upload to MIT keyserver
gpg --keyserver hkps://pgp.mit.edu --send-keys $GPG_KEY_ID
```

### Option B: Web Upload (If Command Line Fails)

1. **keys.openpgp.org**:
   - Go to: https://keys.openpgp.org/upload
   - Upload `public-key.asc` or paste the key
   - **Important**: Check your email and verify!

2. **keyserver.ubuntu.com**:
   - Go to: https://keyserver.ubuntu.com/
   - Click "Submit Key"
   - Paste your public key

3. **pgp.mit.edu**:
   - Go to: https://pgp.mit.edu/
   - Click "Submit a key"
   - Paste your public key

## Step 5: Verify Key Upload

Wait 5-10 minutes for propagation, then verify:

```bash
# Try to retrieve your key
gpg --keyserver hkps://keys.openpgp.org --recv-keys $GPG_KEY_ID

# Search for your key by email
gpg --keyserver hkps://keys.openpgp.org --search-keys your.email@example.com

# Check on the web
# Visit: https://keys.openpgp.org/ and search for your email or key ID
```

## Step 6: Configure Gradle

The `build.gradle.kts` is already configured to use GPG signing with two modes:

### Local Development (Automatic)
When `GPG_PRIVATE_KEY` is not set, uses your system GPG:
```bash
# Just run gradle commands
./gradlew publish
```

### CI/GitHub Actions (Manual Setup)
Set these secrets in GitHub:

1. **GPG_PRIVATE_KEY**:
   ```bash
   gpg --armor --export-secret-keys $GPG_KEY_ID
   ```
   Copy the entire output including BEGIN/END lines

2. **GPG_PASSPHRASE**: Your GPG key passphrase

3. **MAVEN_CENTRAL_USERNAME**: From https://central.sonatype.com

4. **MAVEN_CENTRAL_PASSWORD**: From https://central.sonatype.com

## Step 7: Test Signing Locally

```bash
# Clean and build with signing
./gradlew clean build

# Check if .asc files are created
ls -la build/libs/*.asc
ls -la build/publications/mavenJava/*.asc
```

You should see:
- `turso-0.0.1.jar.asc`
- `turso-0.0.1-sources.jar.asc`
- `turso-0.0.1-javadoc.jar.asc`
- `pom-default.xml.asc`

## Step 8: Publish to Maven Central

```bash
# Set Maven Central credentials
source setup-gpg-env.sh

# Publish
./gradlew publishToMavenCentral
```

## Troubleshooting

### "No secret key" Error
```bash
# Verify your key exists
gpg --list-secret-keys
```

### "Invalid passphrase" Error
- Make sure you remember your GPG passphrase
- For local development, GPG will prompt you for the passphrase

### "Key not found on server" Error
- Wait 10-30 minutes for keyserver propagation
- Verify your key is uploaded: Search at https://keys.openpgp.org/
- For keys.openpgp.org: Check your email for verification link!

### "No route to host" Error
- Use `hkps://` instead of `hkp://`
- Use web upload instead of command line
- Check firewall/proxy settings

## Key Maintenance

### Extend Key Expiration
```bash
# Edit your key
gpg --edit-key $GPG_KEY_ID

# At the gpg> prompt:
gpg> expire
# Follow prompts to set new expiration
gpg> save

# Re-upload to keyservers
gpg --keyserver hkps://keys.openpgp.org --send-keys $GPG_KEY_ID
```

### Revoke a Key (If Compromised)
```bash
# Generate revocation certificate
gpg --output revoke.asc --gen-revoke $GPG_KEY_ID

# Import it
gpg --import revoke.asc

# Upload to keyservers
gpg --keyserver hkps://keys.openpgp.org --send-keys $GPG_KEY_ID
```

## Best Practices

1. ✅ **Backup your private key** in a secure location
   ```bash
   gpg --armor --export-secret-keys $GPG_KEY_ID > private-key-backup.asc
   ```

2. ✅ **Store your passphrase** in a password manager

3. ✅ **Set an expiration date** (2-3 years is recommended)

4. ✅ **Upload to multiple keyservers** (at least 2)

5. ✅ **Verify email on keys.openpgp.org** (required for public search)

6. ✅ **Test locally before CI** to ensure signing works

## Quick Reference

```bash
# Your key information
export GPG_KEY_ID="FAF892C136A2C931"
export GPG_EMAIL="seonwoo960000.kim@gmail.com"

# Common commands
gpg --list-keys                    # List public keys
gpg --list-secret-keys            # List private keys
gpg --armor --export $GPG_KEY_ID  # Export public key
gpg --keyserver hkps://keys.openpgp.org --send-keys $GPG_KEY_ID  # Upload
```

## References

- [Maven Central GPG Requirements](https://central.sonatype.org/publish/requirements/gpg/)
- [GnuPG Documentation](https://gnupg.org/documentation/)
- [keys.openpgp.org](https://keys.openpgp.org/)
- [Ubuntu Keyserver](https://keyserver.ubuntu.com/)
