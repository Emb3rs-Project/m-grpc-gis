import json
import os
from concurrent import futures
from pathlib import Path

import dotenv
import grpc
import jsonpickle

from base.wrappers import SimulationWrapper
from gis.gis_pb2 import CreateNetworkInput, CreateNetworkOutput, OptimizeNetworkInput, OptimizeNetworkOutput
from gis.gis_pb2_grpc import GISModuleServicer, add_GISModuleServicer_to_server
from module.functions.create_network import run_create_network
from module.functions.optimize_network import run_optimize_network
from module.utilities.kb import KB
from module.utilities.kb_data import kb

dotenv.load_dotenv()
PROJECT_PATH = str(Path.cwd())


class GISModule(GISModuleServicer):
    def create_network(self, request: CreateNetworkInput, context) -> CreateNetworkOutput:
        input_dict = {
            "platform": jsonpickle.decode(request.platform),
            "cf-module": jsonpickle.decode(request.cf_module),
            "teo-module": jsonpickle.decode(request.teo_module)
        }
        with SimulationWrapper(project_path=PROJECT_PATH):
            output = run_create_network(input_data=input_dict, KB=KB(data=kb))
        return CreateNetworkOutput(
            nodes=json.dumps(output['nodes']),
            edges=json.dumps(output['edges']),
            demand_list=json.dumps(output['demand_list']),
            supply_list=json.dumps(output['supply_list']),
        )

    def optimize_network(self, request: OptimizeNetworkInput, context) -> OptimizeNetworkOutput:
        input_dict = {
            "platform": jsonpickle.decode(request.platform),
            "cf-module": jsonpickle.decode(request.cf_module),
            "teo-module": jsonpickle.decode(request.teo_module),
            "gis-module": jsonpickle.decode(request.gis_module),
        }
        with SimulationWrapper(project_path=PROJECT_PATH):
            output = run_optimize_network(input_data=input_dict, KB=KB(data=kb))
        return OptimizeNetworkOutput(
            res_sources_sinks=json.dumps(output['res_sources_sinks']),
            sums=json.dumps(output['sums']),
            losses_cost_kw=json.dumps(output['losses_cost_kw']),
            network_solution_nodes=json.dumps(output['network_solution_nodes']),
            network_solution_edges=json.dumps(output['network_solution_edges']),
            potential_edges=json.dumps(output['potential_edges']),
            potential_nodes=json.dumps(output['potential_nodes']),
            selected_agents=json.dumps(output['selected_agents']),
            names_dict=json.dumps(output['names_dict']),
            report=output['report'],
        )


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[('grpc.max_send_message_length', -1), ('grpc.max_receive_message_length', -1)],
    )
    add_GISModuleServicer_to_server(GISModule(), server)

    server.add_insecure_port(f"{os.getenv('GRPC_HOST')}:{os.getenv('GRPC_PORT')}")
    print(f"GIS Module Listening at {os.getenv('GRPC_HOST')}:{os.getenv('GRPC_PORT')}")

    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
