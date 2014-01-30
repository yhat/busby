fs = require "fs"
path = require "path"
package_json = JSON.parse fs.readFileSync path.join(__dirname, "../package.json")
batch = require "./batch"
doc = """

Usage:
    batch-stream [options] <uri> <filename> <output_file> <username> <apikey>

Options:
    --help
    --version

Description:
    #{package_json.description}

"""
{docopt} = require "docopt", version: package_json.version
options = docopt doc

# handle arguments
batch options["<uri>"], options["<filename>"], options["<output_file>"], options["<username>"], options["<apikey>"]