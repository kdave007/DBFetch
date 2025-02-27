#!/usr/bin/env python3

import sys
import os
from controllers.dbf_controller import DBFController
from views.cli_interface import CLIInterface
from utils.config_loader import ConfigLoader

def main():
    try:
        # Initialize configuration
        config = ConfigLoader()
        config.load()

        # Initialize the DBF controller
        controller = DBFController(config)

        # Initialize CLI interface
        cli = CLIInterface(controller)

        # Start the application
        cli.run()

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()