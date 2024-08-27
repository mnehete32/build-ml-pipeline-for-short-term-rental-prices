#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb

import os
import pandas as pd
import tempfile

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("downloading dataset...")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("reading dataset...")
    df = pd.read_csv(artifact_local_path)

    logger.info("cleaning dataset...")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    
    # # Convert last_review to datetime
    # df['last_review'] = pd.to_datetime(df['last_review'])

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    with tempfile.TemporaryDirectory() as temp_dir:
        output_artifact_path = os.path.join(temp_dir, args.output_artifact)
        df.to_csv(output_artifact_path, index=False)
        
        artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description,
        )
        artifact.add_file(output_artifact_path)
        run.log_artifact(artifact)
        artifact.wait()



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type= str,
        help= "name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type= str,
        help= "name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help= "type of the artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help= "cleaned data",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help= "minimun price of the house",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help= "maximum price of the house",
        required=True
    )


    args = parser.parse_args()

    go(args)
