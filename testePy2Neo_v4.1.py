#!/usr/bin/env python
#
# script simples, para testar a extracao de dados de um documento xml
# a partir de uma sugestao em outro contexto, de machine learning, no stackoverflow
# respondendo a questao 6378350
# versao modificada do testeRBIE-7
#
import codecs
import pprint
import xml.etree.ElementTree as ET
from py2neo import neo4j, cypher
from py2neo import node,  rel
# calls database service of Neo4j
#
graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
#
# empty database
graph_db.clear()	# not sure if this really works...let's check...
#
# following nigel small suggestion in http://stackoverflow.com
#
titulo_index = graph_db.get_or_create_index(neo4j.Node, "titulos")
autores_index = graph_db.get_or_create_index(neo4j.Node, "autores")
keyword_index = graph_db.get_or_create_index(neo4j.Node,  "keywords")
dataPub_index = graph_db.get_or_create_index(neo4j.Node, "datas")
#
# indexando os relacionamentos
#
r_autoria_ndx = graph_db.get_or_create_index(neo4j.Relationship, "Autoria")
r_keyword_ndx= graph_db.get_or_create_index(neo4j.Relationship, "Tem_keyword")
r_publicacao_ndx = graph_db.get_or_create_index(neo4j.Relationship, "Publicacao")
#
# processa o documento xml 
#
tree = ET.ElementTree(file='DatasetRBIE.xml')
raiz=tree.getroot()
def getInfoNodes(child, searchNode):
    # pega os filhos do nodo
    children = child.getchildren()
    #
    # se o nodo tiver um ou mais filhos
    if len(children) >= 1:
        # estrutura de dados para guardar os resultados
        # no caso uma lista
        info = []
        # loop em todos os filhos
        for child in children:
            # chamada recursiva da funcao
            resultado = getInfoNodes(child,  searchNode)
            # junta os resultados
            info.extend(resultado)
        # retorna os resultados
        return info
    # Caso base 1: nao tem filhos e eh o 'searchNode'
    elif len(children) == 0 and child.tag == searchNode:
        # retorna o texto do nodo, dentro da lista
        return [child.text]
    # caso base 2: nao tem filho 
    else:
        # retorna o texto
        return [child.text]
        
chaveBusca=['publicationDate','title','authors','author','name','keywords','keyword']
listaSaida = []
for registro in raiz:
    # pega os filhos do nodo 'records'
    children = registro.getchildren()
    # tem que saber se nao esta vazio...
    if children:
        # loop pelos nodos filhos
        for child in children:
            for chave in chaveBusca:
                searchNode = chave
                # a chave bate com a tag
                lista = []
                if child.tag == chave:
                    # chama a funcao getInfoNodes e passa o resultado para a lista
                    lista = getInfoNodes(child, searchNode)
                    listaSaida.append(lista)
                    #print (lista)
                        
                        
    listafinal = listaSaida
# split list
pedacos = [listafinal[i:i+4] for i in range(0, len(listafinal), 4)]
print len(pedacos)
# 
# cria listas para colecionar os nodos indexados???
# pode ser melhorado em um codigo otimizado
#
dataPub_nodes = []
titulo_nodes = []
autores_nodes = []
keyword_nodes = []
#
# creates lists to store indexed relationships
r_autoria = []		# stores indexed autoria relationship
r_keyword = []		# idem for keyword
r_publicacao = []	# idem for publicacao

for i in range(0, len(pedacos)):
	# preenche dataPub_nodes e titulo_nodes com conteudo
	dataPub_nodes.append(dataPub_index.get_or_create("data", pedacos[i][0], {"data": pedacos[i][0]}))
	titulo_nodes.append(titulo_index.get_or_create("titulo", pedacos[i][1], {"titulo": pedacos[i][1]}))	# title node...
	# cria o relacionamento publicado em
	# now, publication is indexed
	r_publicacao.append(r_publicacao_ndx.get_or_create("Publicacao", "publicado_em",(titulo_nodes[i], "publicado_em", dataPub_nodes[i])))
	#publicacao = graph_db.get_or_create_relationships((titulo_nodes[i], "publicado_em", dataPub_nodes[i]))
	# processamento da sublista dos autores
	for j in range(0,  len(pedacos[i][2])):  # o indice corre na lista de nivel 3, de autores
		
		# preenche a lista dos nodos indexados autores_nodes 
		autores_nodes.append(autores_index.get_or_create("autor", pedacos[i][2][j], {"autor":pedacos[i][2][j]}))
		
		# cria o relacionamento tem como autor...
		r_autoria.append(r_autoria_ndx.get_or_create("Autoria", "tem_como_autor", (titulo_nodes[i], "tem_como_autor", autores_nodes[j])))
		#autoria = graph_db.get_or_create_relationships((titulo_nodes[i], "tem_como_autor", autores_nodes[j]))
		# mesma logica...
		for k in range(0, len(pedacos[i][3])):
			if pedacos[i][3][k]== None:
				# caso de existirem nodos nulos...
				keyword_nodes.append(keyword_index.get_or_create("keyword", "Indefinida", {"keyword":"Indefinida"}))
			else:
				keyword_nodes.append(keyword_index.get_or_create("keyword", pedacos[i][3][k], {"keyword":pedacos[i][3][k]}))
				# cria o relacionamento 'tem_como_keyword'
				r_keyword.append(r_keyword_ndx.get_or_create("Tem_keyword", "tem_como_keyword", (titulo_nodes[i], "tem_como_keyword", keyword_nodes[k])))	
				#tem_keyword = graph_db.get_or_create_relationships((titulo_nodes[i], "tem_como_keyword", keyword_nodes[k]))
