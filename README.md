#  Air Quality API

Air Quality API is an API to connect our [web](https://main.d2bs3bzajz7n0u.amplifyapp.com/) and our database. This is a project with PUCP and Fondecyt
 
## Getting Started with installation

### Ubuntu, Mac & Windows
Clone or download the project to the device where it will be used.

```
git clone https://github.com/qAIRa/qAIRaMapAPI-OpenSource.git
```

### Prerequisites Ubuntu & Mac
Now you have to open terminal
You must have an isolated environment by executing the following command:

```
python3 -m pip install virtualenv

python3 -m virtualenv venv

source venv/bin/activate

```
### Prerequisites Windows
Now you have to open CMD with administrator permissions
You must have an isolated environment by executing the following command: 

```
py -3 -m pip install virtualenv

py -3 -m virtualenv venv

source venv\Scripts\activate

```

### After that (for all Operating Systems)

Now you have to run this command to install all require libraries

```
pip install -r requirements.txt
```

Also, to set environment variables you need to run this command:


```

export SQLALCHEMY_DATABASE_URI='******'

```
Now you are ready to run the main code

```
python run.py
```

If everything went well, the following should come out

```
* Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)

```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## FAQs

If you're encountering some oddities in the API, here's a list of resolutions to some of the problems you may be experiencing.

Why am I getting a 404?
The request could not be understood by the server due to malformed syntax. The client should not repeat the request without modifications.
I recommend you to see the response message it could be something like this: {"error":"No target environmental_station_id in given json"}

Why am I getting a 405?

Not Allowed - It occurs when web server is configured in a way that does not allow you to perform an action for a particular URL. Maybe when you run an endpoint with GET instead of POST

Why am I not seeing all my results?
Most API calls accessing a list of resources (e.g., users, issues, etc.). If you're making requests and receiving an incomplete set of results, a response is specified in an unsupported content type.

Why am I getting a 500?
Server Mistake - Indicates that something went wrong on the server that prevent the server from fulfilling the request.

