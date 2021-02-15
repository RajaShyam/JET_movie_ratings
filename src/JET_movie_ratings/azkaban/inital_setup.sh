#!/usr/bin/env bash

cd /mnt/tmp/;
git clone https://github.com/RajaShyam/JET_movie_ratings.git
cd JET_movie_ratings;
sudo python3 setup.py clean build install
echo "Initial Setup complete!!"