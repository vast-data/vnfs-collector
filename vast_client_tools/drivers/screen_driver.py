import argparse
from tabulate import tabulate
from vast_client_tools.drivers.base import DriverBase


class ScreenDriver(DriverBase):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--table-format', action="store_true", help='Tabulated output.')

    def __str__(self):
        return f"{self.__class__.__name__}(table_format={self.table_format})"

    async def setup(self, args=(), namespace=None):
        args = await super().setup(args, namespace)
        self.table_format = args.table_format
        self.logger.info(f"{self} has been initialized.")

    async def store_sample(self, data):
        if self.table_format:
            columns = list(data[0].keys())
            # Transpose the data to create a table with field names and their values across samples
            transposed_data = [
                [col] + [entry.get(col, None) for entry in data] for col in columns
            ]
            # Create headers
            headers = ["Field"] + [f"Sample {ind}" for ind in range(len(columns))]
            # Print the table
            data = tabulate(transposed_data, headers=headers, tablefmt="rounded_grid")
        self.logger.info(f">>>\n{data}")
