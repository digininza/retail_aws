#!/bin/bash
# EMR bootstrap action (Section 15). Installs job-specific Python
# dependencies on every node before Spark starts. Kept minimal — EMR's
# built-in Spark/Hadoop stack covers everything else.
set -euo pipefail

sudo python3 -m pip install --no-cache-dir \
    pandas==2.2.2 \
    pyyaml==6.0.1 \
    boto3==1.34.144

echo "Bootstrap complete: retail-aws-data-platform EMR dependencies installed."
