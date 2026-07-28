# Cloud-to-Cloud Upload

GitHub Action that streams a public, byte-range-enabled source URL to TransferIt without writing the media file to disk.

The workflow accepts an R2 Download URL, filename, Sheet row number, and request ID. It publishes a small result artifact containing the final TransferIt link.

This repository is safe to make public. Keep `main.py`, Google credentials, cookies, WordPress credentials, and `transferit_github.json` outside this repository.
