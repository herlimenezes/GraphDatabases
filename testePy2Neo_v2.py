#!/usr/bin/env python
#

from py2neo import neo4j, cypher
from py2neo import node,  rel

# calls database service of Neo4j
#
graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
#
# following nigel small suggestion in http://stackoverflow.com
#
titulo_index = graph_db.get_or_create_index(neo4j.Node, "titulo")
autores_index = graph_db.get_or_create_index(neo4j.Node, "autores")
keyword_index = graph_db.get_or_create_index(neo4j.Node,  "keywords")
dataPub_index = graph_db.get_or_create_index(neo4j.Node, "data")
#
# to begin, database clear...

graph_db.clear()	# not sure if this really works...let's check...
#
# the big list, next version this is supposed to be read from a file...
#
listaBase = [['2007-12-18'], ['RECONHECIMENTO E AGRUPAMENTO DE OBJETOS DE APRENDIZAGEM SEMELHANTES'], ['Raphael Ghelman', 'Sean W. M. Siqueira', 'Maria Helena L. B. Braz', 'Rubens N. Melo'], ['Objetos de Aprendizagem', u'Personaliza\xe7\xe3o', u'Perfil do Usu\xe1rio', u'Padr\xf5es de Metadados', u'Vers\xf5es de Objetos de Aprendizagem', 'Agrupamento de Objetos Similares'], ['2007-12-18'], [u'LOCPN: REDES DE PETRI COLORIDAS NA PRODU\xc7\xc3O DE OBJETOS DE APRENDIZAGEM'], [u'Maria de F\xe1tima Costa de Souza', 'Danielo G. Gomes', 'Giovanni Cordeiro Barroso', 'Cidcley Teixeira de Souza', u'Jos\xe9 Aires de Castro Filho', 'Mauro Cavalcante Pequeno', 'Rossana M. C. Andrade'], ['Objetos de Aprendizagem', 'Modelo de Processo', 'Redes de Petri Colorida', u'Especifica\xe7\xe3o formal'], ['2007-12-18'], [u'COMPUTA\xc7\xc3O M\xd3VEL E UB\xcdQUA NO CONTEXTO DE UMA GRADUA\xc7\xc3O DE REFER\xcaNCIA'], ['Jorge Barbosa', 'Rodrigo Hahn', 'Solon Rabello', u'S\xe9rgio Crespo C. S. Pinto', u'D\xe9bora Nice Ferrari Barbosa'], [u'Computa\xe7\xe3o M\xf3vel e Ub\xedqua', u'Gradua\xe7\xe3o de Refer\xeancia', u' Educa\xe7\xe3o Ub\xedqua']]
pedacos = [listaBase[i:i+4] for i in range(0, len(listaBase), 4)] # pedacos = chunks
# 
# lists to collect indexed nodes: is it really useful???
# let's think about it when optimizing code...
dataPub_nodes = []
titulo_nodes = []
autores_nodes = []
keyword_nodes = []
#
# 
for i in range(0, len(pedacos)):
	# fill dataPub_nodes and titulo_nodes with content.
	
	#dataPub_nodes.append(dataPub_index.get_or_create("data", pedacos[i][0], {"data":pedacos[i][0]}))	# Publication date nodes...
	dataPub_nodes.append(dataPub_index.get_or_create("data", str(pedacos[i][0]).strip('[]'), {"data":str(pedacos[i][0]).strip('[]')}))
	# ------------------------------- Exception raised here... --------------------------------
	# The debugged program raised the exception unhandled TypeError
	#"(1649 {"titulo":["RECONHECIMENTO E AGRUPAMENTO DE OBJETOS DE APRENDIZAGEM SEMELHANTES"]})"
	#File: /usr/lib/python2.7/site-packages/py2neo-1.5.1-py2.7.egg/py2neo/neo4j.py, Line: 472
	# ------------------------------  What happened??? ----------------------------------------

	titulo_nodes.append(titulo_index.get_or_create("titulo", str(pedacos[i][1]).strip('[]'), {"titulo":str(pedacos[i][1]).strip('[]')}))	# title node...
	
	# creates relationship publicacao
	publicacao = graph_db.get_or_create_relationships(titulo_nodes[i], "publicado_em", dataPub_nodes[i])
	
	# now processing autores sublist and collecting in autores_nodes
	#
	for j in range(0,  len(pedacos[i][2])):
		# fill autores_nodes list
		autores_nodes.append(autores_index.get_or_create("autor", pedacos[i][2][j], {"autor":pedacos[i][2][j]}))
		# creates autoria relationship...
		#
		autoria = graph_db.get_or_create_relationships(titulo_nodes[i], "tem_como_autor", autores_nodes[j])
		# same logic...
		#
		for k in range(0, len(pedacos[i][3])):
			keyword_nodes.append(keyword_index.get_or_create("keyword", pedacos[i][3][k]))
			# cria o relacionamento 'tem_como_keyword'
			tem_keyword = graph_db.get_or_create_relationships(titulo_nodes[i], "tem_como_keyword", keyword_nodes[k])
