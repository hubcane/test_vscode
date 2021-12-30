import networkx as nx
import numpy as np 
import matplotlib.pyplot as plt
from networkx.algorithms.community import girvan_newman

G = nx.Graph()
G.add_node(1)
G.add_nodes_from([2,3])
G.add_edge(1,2)

print(list(nx.connected_components(G)))

