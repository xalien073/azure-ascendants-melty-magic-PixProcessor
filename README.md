Commands to run the PixProcessor:

Install Azure Functions Core Tools
This is essential for running and debugging Azure Functions locally:

# On Windows (using Chocolatey)
choco install azure-functions-core-tools-4 --params "'/x64'" -y


For azurite, which is Azure storage account emulator:

npm install -g azurite

npm list -g azurite

azurite

For installing PixProcessor's dependencies, run inside PixProcessor's root directory:

npm install

func start


Commands to initiate & start new Azure function app with node as a worker runtime in local machine:

func init --worker-runtime node

func new --template "Azure Event Hub trigger" --name PixProcessor
