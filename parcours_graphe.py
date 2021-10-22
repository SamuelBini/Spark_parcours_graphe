
from pyspark import SparkContext

sc = SparkContext(master="local[2]", appName="Parcours en largeur de graphe")


def mapper(tupleA : tuple):
    """
    Fonction mapper
        Elle reçoit en entrée un tuple et envoie en sortie une liste de couples cle-valeur
    """
    key, value = tupleA[0], tupleA[1]
    new_graph = []
    if value[1] == "GRIS":
        for fils in value[0].split(','):
            if fils == "":
                continue
            fils_couleur = "GRIS"
            fils_profondeur = int(value[2]) + 1
            fils_t = ["", fils_couleur, fils_profondeur]
            new_graph.append((fils, fils_t))
        value[1] = "NOIR"
    new_graph.append((key, value))
    return new_graph


def reducer(list_values):
    """
    Fonction mapper
        Elle reçoit en entrée une liste de couple cle-valeur qui seront traités
    """
    global compteur
    h_children, h_depth, h_color = [], -1, "BLANC"
    dic = {
        "BLANC": 0,
        "GRIS": 1,
        "NOIR": 2
    }
    for val in list_values:
        if len(val[0].split(",")) >= len(h_children):
            h_children = val[0].split(",")
        if int(val[2]) > h_depth:
            h_depth = int(val[2])
        if dic[val[1]] > dic[h_color]:
            h_color = val[1]
    if h_color != "NOIR":
        compteur.add(1)
    new_node = [",".join(h_children), h_color, h_depth]
    return new_node


#compteur = sc.accumulator(1)
graph_input = sc.textFile("/graphe.txt")
graph_input.collect() 

graph_splitted1 = graph_input.map(lambda x: x.split("\t"))
graph_splitted1.collect() 

graph_splitted2 = graph_splitted1.mapValues(lambda x: x.split('|'))
graph_splitted2 = graph_splitted2.sortByKey(lambda x: x[0])
graph_splitted2.collect() 

compteur = sc.accumulator(1)
iteration=0

while compteur.value > 0:
    iteration=iteration+1
    print("-----------------", "ITERATION ",iteration, "-----------------")
    compteur = sc.accumulator(0)
    
    #   Phase map
    node1 = graph_splitted2.flatMap(mapper)
    print("After map :")
    node1.collect()

    #   Phase shuffle
    node2 = node1.groupByKey()

    #   Phase reduce
    node3 = node2.mapValues(reducer)
    node3 = node3.sortBy(lambda x: x[0])
    print("After reduce :")
    node3.sortByKey(lambda x: x[0]).collect()
    graph_splitted2=node3

graph_splitted2.saveAsTextFile("graph_results")
