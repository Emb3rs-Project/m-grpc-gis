from base64 import decode
import os
from concurrent import futures

import dotenv
import grpc
import jsonpickle

from gis.gis_pb2 import CreateNetworkInput, CreateNetworkOutput, OptimizeNetworkInput, OptimizeNetworkOutput
from gis.gis_pb2_grpc import GISModuleServicer, add_GISModuleServicer_to_server
from module.functions.create_network import run_create_network
from module.functions.optimize_network import run_optimize_network
from module.utilities.kb import KB
from module.utilities.kb_data import kb

dotenv.load_dotenv()

class GISModule(GISModuleServicer):

  def __init__(self) -> None:
      super().__init__()

  def RunCreateNetwork(self, request: CreateNetworkInput, context) -> CreateNetworkOutput:
    input_dict = {
      "platform" : jsonpickle.decode(request.platform),
      "cf-module": jsonpickle.decode(request.cf_module),
      "teo-module": jsonpickle.decode(request.teo_module)
    }
    output = run_create_network(input_data=input_dict, KB=KB(data=kb))

    return CreateNetworkOutput(
      road_nw = jsonpickle.encode(output['road_nw'], unpicklable=True),
      n_demand_dict = jsonpickle.encode(output['n_demand_dict'], unpicklable=True),
      n_supply_dict = jsonpickle.encode(output['n_supply_dict'], unpicklable=True),
    )

  def RunOptimizeNetwork(self, request:OptimizeNetworkInput, context) -> OptimizeNetworkOutput:
    input_dict = {
      "platform" : jsonpickle.decode(request.platform),
      "cf-module": jsonpickle.decode(request.cf_module),
      "teo-module": jsonpickle.decode(request.teo_module),
      "gis-module": jsonpickle.decode(request.gis_module),
    }
    output = run_optimize_network(input_data=input_dict, KB=KB(data=kb))

    return OptimizeNetworkOutput(
      res_sources_sinks = jsonpickle.encode(output['res_sources_sinks'], unpicklable=True),
      sums = jsonpickle.encode(output['sums'], unpicklable=True),
      losses_cost_kw = jsonpickle.encode(output['losses_cost_kw'], unpicklable=True),
      network_solution_nodes = jsonpickle.encode(output['network_solution_nodes'], unpicklable=True),
      network_solution_edges = jsonpickle.encode(output['network_solution_edges'], unpicklable=True),
      potential_edges = jsonpickle.encode(output['potential_edges'], unpicklable=True),
      potential_nodes = jsonpickle.encode(output['potential_nodes'], unpicklable=True),
      selected_agents = jsonpickle.encode(output['selected_agents'], unpicklable=True),
    )


def serve():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  add_GISModuleServicer_to_server(GISModule, server)

  server.add_insecure_port(f"{os.getenv('GRPC_HOST')}:{os.getenv('GRPC_PORT')}")

  print(f"Market module Listening at {os.getenv('GRPC_HOST')}:{os.getenv('GRPC_PORT')}")

  server.start()
  server.wait_for_termination()

if __name__ == '__main__':
  serve()
  