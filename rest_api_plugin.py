__author__ = "lliu12, linhgao"
__version__ = "1.0.0"

from airflow.models import DagBag, DagModel
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from airflow import settings
from flask import Blueprint, request, Response
from flask_login.utils import _get_user

import airflow
import logging
import os
import socket
import json
from flask_appbuilder import (
    expose as app_builder_expose,
    BaseView as AppBuilderBaseView,
)
from flask_jwt_extended.view_decorators import jwt_required, verify_jwt_in_request

"""Location of the REST Endpoint
Note: Changing this will only effect where the messages are posted to on the web interface
and will not change where the endpoint actually resides"""
rest_api_endpoint = "/rest_api/api"

# Getting Versions and Global variables
hostname = socket.gethostname()
airflow_version = airflow.__version__
rest_api_plugin_version = __version__


# # Getting configurations from airflow.cfg file
# airflow_webserver_base_url = configuration.get('webserver', 'BASE_URL')
# airflow_dags_folder = configuration.get('core', 'DAGS_FOLDER')
# rbac_authentication_enabled = configuration.getboolean("webserver", "RBAC")
# store_serialized_dags = configuration.getboolean('core', 'store_serialized_dags')
airflow_webserver_base_url = "http://localhost:8080"
airflow_dags_folder = "/opt/airflow/dags"
rbac_authentication_enabled = True
store_serialized_dags = True


if rbac_authentication_enabled:
    rest_api_endpoint = "/rest_api/api"


apis_metadata = [
    {
        "name": "hello_gang",
        "description": "HELLO_GANG MAN",
        "http_method": "GET",
        "arguments": [],
    },
    {
        "name": "deploy_dag",
        "description": "Deploy a new DAG File to the DAGs directory",
        "http_method": "POST",
        "form_enctype": "multipart/form-data",
        "arguments": [],
        "post_arguments": [
            {
                "name": "dag_file",
                "description": "Python file to upload and deploy",
                "form_input_type": "file",
                "required": True,
            },
            {
                "name": "force",
                "description": "Whether to forcefully upload the file if the file already exists or not",
                "form_input_type": "checkbox",
                "required": False,
            },
            {
                "name": "pause",
                "description": "The DAG will be forced to be paused when created.",
                "form_input_type": "checkbox",
                "required": False,
            },
            {
                "name": "unpause",
                "description": "The DAG will be forced to be unpaused when created.",
                "form_input_type": "checkbox",
                "required": False,
            },
        ],
    },
    {
        "name": "upload_file",
        "description": "upload a new File to the specified folder",
        "http_method": "POST",
        "form_enctype": "multipart/form-data",
        "arguments": [],
        "post_arguments": [
            {
                "name": "file",
                "description": "uploaded file",
                "form_input_type": "file",
                "required": True,
            },
            {
                "name": "force",
                "description": "Whether to forcefully upload the file if the file already exists or not",
                "form_input_type": "checkbox",
                "required": False,
            },
            {
                "name": "path",
                "description": "the path of file",
                "form_input_type": "text",
                "required": False,
            },
        ],
    },
]


# Function used to validate the JWT Token
def jwt_token_secure(func):
    def jwt_secure_check(arg):
        logging.info("Rest_API_Plugin.jwt_token_secure() called")
        if _get_user().is_anonymous is False and rbac_authentication_enabled is True:
            return func(arg)
        elif rbac_authentication_enabled is False:
            return func(arg)
        else:
            verify_jwt_in_request()
            return jwt_required(func(arg))

    return jwt_secure_check


class ApiResponse:
    def __init__(self):
        pass

    STATUS_OK = 200
    STATUS_BAD_REQUEST = 400
    STATUS_UNAUTHORIZED = 401
    STATUS_NOT_FOUND = 404
    STATUS_SERVER_ERROR = 500

    @staticmethod
    def standard_response(status, response_obj):
        json_data = json.dumps(response_obj)
        resp = Response(json_data, status=status, mimetype="application/json")
        return resp

    @staticmethod
    def success(response_obj={}):
        response_obj["status"] = "success"
        return ApiResponse.standard_response(ApiResponse.STATUS_OK, response_obj)

    @staticmethod
    def error(status, error):
        return ApiResponse.standard_response(status, {"error": error})

    @staticmethod
    def bad_request(error):
        return ApiResponse.error(ApiResponse.STATUS_BAD_REQUEST, error)

    @staticmethod
    def not_found(error="Resource not found"):
        return ApiResponse.error(ApiResponse.STATUS_NOT_FOUND, error)

    @staticmethod
    def unauthorized(error="Not authorized to access this resource"):
        return ApiResponse.error(ApiResponse.STATUS_UNAUTHORIZED, error)

    @staticmethod
    def server_error(error="An unexpected problem occurred"):
        return ApiResponse.error(ApiResponse.STATUS_SERVER_ERROR, error)


def get_baseview():
    return AppBuilderBaseView


class REST_API(get_baseview()):
    """API View which extends either flask AppBuilderBaseView or flask AdminBaseView"""

    @staticmethod
    def is_arg_not_provided(arg):
        return arg is None or arg == ""

    # Get the DagBag which has a list of all the current Dags
    @staticmethod
    def get_dagbag():
        print("settings.DAGS_FOLDER: ", settings.DAGS_FOLDER)
        return DagBag(
            dag_folder=settings.DAGS_FOLDER, store_serialized_dags=store_serialized_dags
        )

    @staticmethod
    def get_argument(request, arg):
        return request.args.get(arg) or request.form.get(arg)

    @app_builder_expose("/")
    def list(self):
        logging.info("REST_API.list() called")

        # get the information that we want to display on the page regarding the dags that are available
        dagbag = self.get_dagbag()
        dags = []
        for dag_id in dagbag.dags:
            orm_dag = DagModel.get_current(dag_id)
            dags.append(
                {
                    "dag_id": dag_id,
                    "is_active": (not orm_dag.is_paused)
                    if orm_dag is not None
                    else False,
                }
            )

        return self.render_template(
            "/opt/airflow/rest_api_plugin/index.html ",
            dags=dags,
            airflow_webserver_base_url=airflow_webserver_base_url,
            rest_api_endpoint=rest_api_endpoint,
            apis_metadata=apis_metadata,
            airflow_version=airflow_version,
            rest_api_plugin_version=rest_api_plugin_version,
            rbac_authentication_enabled=rbac_authentication_enabled,
        )

    # '/api' REST Endpoint where API requests should all come in
    @csrf.exempt  # Exempt the CSRF token
    # @admin_expose("/api", methods=["GET", "POST"])  # for Flask Admin
    @app_builder_expose("/api", methods=["GET", "POST"])  # for Flask AppBuilder
    # @jwt_token_secure  # On each request
    def api(self):
        # Get the api that you want to execute
        api = self.get_argument(request, "api")

        # Validate that the API is provided
        if self.is_arg_not_provided(api):
            logging.warning("api argument not provided")
            return ApiResponse.bad_request("API should be provided")

        api = api.strip().lower()
        logging.info("REST_API.api() called (api: " + str(api) + ")")

        # Get the api_metadata from the api object list that correcsponds to the api we want to run to get the metadata.
        api_metadata = None
        for test_api_metadata in apis_metadata:
            if test_api_metadata["name"] == api:
                api_metadata = test_api_metadata
        if api_metadata is None:
            logging.info(
                "api '"
                + str(api)
                + "' was not found in the apis list in the REST API Plugin"
            )
            return ApiResponse.bad_request("API '" + str(api) + "' was not found")

        # check if all the required arguments are provided
        missing_required_arguments = []
        dag_id = None
        for argument in api_metadata["arguments"]:
            argument_name = argument["name"]
            argument_value = self.get_argument(request, argument_name)
            if argument["required"]:
                if self.is_arg_not_provided(argument_value):
                    missing_required_arguments.append(argument_name)
            if argument_name == "dag_id" and argument_value is not None:
                dag_id = argument_value.strip()
        if len(missing_required_arguments) > 0:
            logging.warning(
                "Missing required arguments: " + str(missing_required_arguments)
            )
            return ApiResponse.bad_request(
                "The argument(s) " + str(missing_required_arguments) + " are required"
            )

        # Check to make sure that the DAG you're referring to, already exists.
        dag_bag = self.get_dagbag()
        if dag_id is not None and dag_id not in dag_bag.dags:
            logging.info(
                "DAG_ID '"
                + str(dag_id)
                + "' was not found in the DagBag list '"
                + str(dag_bag.dags)
                + "'"
            )
            return ApiResponse.bad_request(
                "The DAG ID '" + str(dag_id) + "' does not exist"
            )

        # Deciding which function to use based off the API object that was requested.
        # Some functions are custom and need to be manually routed to.
        if api == "deploy_dag":
            final_response = self.deploy_dag()
        elif api == "upload_file":
            final_response = self.upload_file()
        elif api == "hello_gang":
            final_response = self.hello_gang()

        return final_response

    def hello_gang(self):
        """Custom Function for a Hello World API."""
        logging.info("Executing custom 'hello_gang' function")
        return ApiResponse.success({"message": "Hello, HUSSELJO GANG!"})

    def deploy_dag(self):
        """Custom Function for the deploy_dag API
        Upload dag file，and refresh dag to session

        args:
            dag_file: the python file that defines the dag
            force: whether to force replace the original dag file
            pause: disabled dag
            unpause: enabled dag
        """
        logging.info("Executing custom 'deploy_dag' function")

        # check if the post request has the file part
        if "dag_file" not in request.files or request.files["dag_file"].filename == "":
            logging.warning("The dag_file argument wasn't provided")
            return ApiResponse.bad_request("dag_file should be provided")
        dag_file = request.files["dag_file"]

        force = True if self.get_argument(request, "force") is not None else False
        logging.info("deploy_dag force upload: " + str(force))

        pause = True if self.get_argument(request, "pause") is not None else False
        logging.info("deploy_dag in pause state: " + str(pause))

        unpause = True if self.get_argument(request, "unpause") is not None else False
        logging.info("deploy_dag in unpause state: " + str(unpause))

        # make sure that the dag_file is a python script
        if dag_file and dag_file.filename.endswith(".py"):
            save_file_path = os.path.join(airflow_dags_folder, dag_file.filename)

            # Check if the file already exists.
            if os.path.isfile(save_file_path) and not force:
                logging.warning("File to upload already exists")
                return ApiResponse.bad_request(
                    "The file '"
                    + save_file_path
                    + "' already exists on host '"
                    + hostname
                )

            logging.info("Saving file to '" + save_file_path + "'")
            dag_file.save(save_file_path)

        else:
            logging.warning(
                "deploy_dag file is not a python file. It does not end with a .py."
            )
            return ApiResponse.bad_request("dag_file is not a *.py file")

        try:
            # import the DAG file that was uploaded
            # so that we can get the DAG_ID to execute the command to pause or unpause it
            import imp

            # get the module name from the file name
            module_name = os.path.splitext(os.path.basename(save_file_path))[0]
            dag_file = imp.load_source(module_name, save_file_path)

        except Exception as e:
            print(e)
            warning = "Failed to get dag_file"
            logging.warning(warning)
            return ApiResponse.server_error("Failed to get dag_file")

        try:
            print(dag_file)
            if dag_file is None or dag_file.dag is None:
                warning = "Failed to get dag"
                logging.warning(warning)
                return ApiResponse.server_error(
                    "DAG File [{}] has been uploaded".format(dag_file)
                )
        except Exception as e:
            print(e)
            warning = "Failed to get dag from dag_file"
            logging.warning(warning)
            return ApiResponse.server_error(
                "Failed to get dag from DAG File [{}]".format(dag_file)
            )

        dag_id = dag_file.dag.dag_id

        logging.info("dag_id: " + dag_id)

        # Refresh dag into session
        dagbag = self.get_dagbag()
        dag = dagbag.get_dag(dag_id)
        session = settings.Session()
        dag.sync_to_db(session=session)
        dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
        logging.info("dag_model:" + str(dag_model))

        dag_model.set_is_paused(is_paused=not unpause)

        return ApiResponse.success(
            {"message": "DAG File [{}] has been uploaded".format(dag_file)}
        )

    def upload_file(self):
        """Custom Function for the upload_file API.
        Upload files to the specified path.
        """
        logging.info("Executing custom 'upload_file' function")

        # check if the post request has the file part
        if (
            "file" not in request.files
            or request.files["file"] is None
            or request.files["file"].filename == ""
        ):
            logging.warning("The file argument wasn't provided")
            return ApiResponse.bad_request("file should be provided")
        file = request.files["file"]

        path = self.get_argument(request, "path")
        if path is None:
            path = airflow_dags_folder

        force = True if self.get_argument(request, "force") is not None else False
        logging.info("deploy_dag force upload: " + str(force))

        # save file
        save_file_path = os.path.join(path, file.filename)

        # Check if the file already exists.
        if os.path.isfile(save_file_path) and not force:
            logging.warning("File to upload already exists")
            return ApiResponse.bad_request(
                "The file '" + save_file_path + "' already exists on host '" + hostname
            )

        logging.info("Saving file to '" + save_file_path + "'")
        file.save(save_file_path)
        return ApiResponse.success(
            {"message": "File [{}] has been uploaded".format(save_file_path)}
        )


rest_api_view = {"category": "Admin", "name": "REST API Plugin", "view": REST_API()}

# Creating Blueprint
rest_api_bp = Blueprint(
    "rest_api_bp",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)

# Creating Blueprint
rest_api_blueprint = Blueprint("rest_api_blueprint", __name__, url_prefix="/rest/api")


class REST_API_Plugin(AirflowPlugin):
    """Creating the REST_API_Plugin which extends the AirflowPlugin so its imported into Airflow"""

    name = "rest_api"
    operators = []
    appbuilder_views = [rest_api_view]
    flask_blueprints = [rest_api_bp, rest_api_blueprint]
    hooks = []
    executors = []
    admin_views = [rest_api_view]
    menu_links = []
