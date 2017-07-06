from gephistreamer import graph
from gephistreamer import streamer
import re
import os
import sys

directory= "tasks/"
fileType = 'transE'
stream = streamer.Streamer(streamer.GephiREST())


#adds the all_data nodes and edges
def addFolder(path):
    try:
        abspath=os.path.join(path, 'all_data')
        if os.path.isfile(abspath): 
            print abspath
            with open(abspath, 'r') as f:
                contents = f.read().splitlines()
            
            for line in contents:
                #get first columb data
                items = re.split(r'\t+', line)
                labels, labels_1, labels_2= re.split(r':+', items[0]), re.split(r':+', items[1]), re.split(r':+', items[2])
                #type = labels[1]
                stream.add_node(graph.Node(labels[2],type=labels[1]), graph.Node(labels_1[2],type=labels_1[1]))
                
                #edges
                stream.add_edge(graph.Edge(labels[2],labels_1[2],custom_property=labels_2[1]))
           
            print 'success'

    except:
        #the .dsStore file
        print("Unexpected error:", sys.exc_info()[0])
        raise  

#adds the transE (relationship) file
def addFolderG(path):
    try:
        abspath=os.path.join(path, 'transE')
        if os.path.isfile(abspath): 
            print abspath
            with open(abspath, 'r') as f:
                contents = f.read().splitlines()
                
            index=0.0
            legnth = len(contents)
            print legnth
            
            for line in contents:
                items = re.split(r'\t+', line)
                #print items
                labels, labels_1, labels_2= re.split(r'_+', items[0]), re.split(r'_+', items[1]), re.split(r'_+', items[2])
        
                node_name = ''.join(labels[2:])
                node_name_1 = ''.join(labels_1[2:])
                
                try:
                    stream.add_node(graph.Node(node_name,type=labels[1]))
                    stream.add_node(graph.Node(node_name_1,type=labels_1[1]))
                    #edges
                    stream.add_edge(graph.Edge(node_name,node_name_1,relationship=labels_2[0]))
        
                    if index%1000==0:
                        print 'progress', index/legnth
                        
                except:
                    pass
                    #print  'item omited'
                index +=1
                
            print 'success'
    except:
        #the .dsStore file
        print("Unexpected error:", sys.exc_info()[0])
        raise   


#add specific nodes ex: Abraham_lincoln
def addSpecificNode(path, element_name):
    try:
        abspath=os.path.join(path, 'transE')
        if os.path.isfile(abspath): 
            print abspath
            with open(abspath, 'r') as f:
                contents = f.read().splitlines()
        
            index=0.0
            legnth = len(contents)
            print legnth
            
            for line in contents:
                items = re.split(r'\t+', line)
                labels, labels_1, labels_2= re.split(r'_+', items[0]), re.split(r'_+', items[1]), re.split(r'_+', items[2])
                
                node_name = ''.join(labels[2:])
                node_name_1 = ''.join(labels_1[2:])
                
                if node_name == element_name:
                    try:
                        stream.add_node(graph.Node(node_name,type=labels[1]))
                        stream.add_node(graph.Node(node_name_1,type=labels_1[1]))
                        #edges
                        stream.add_edge(graph.Edge(node_name,node_name_1,relationship=labels_2[0]))
                    
                        if index%1000==0:
                            print index/legnth
                            
                    except:
                        pass
                        #print  'item omited'
                    index +=1
            print 'success'

    except:
        #the .dsStore file
        print("Unexpected error:", sys.exc_info()[0])
        raise
    
#adds the nodes/edges for whole tasks file
def addDirectory(directory):  
    for folder in os.listdir(directory):
        path=os.path.join(directory, folder)
        addFolder(path)

#addDirectory(directory)     
#addFolderG('concept_personbelongstoorganization')
#addFolder('concept_personbelongstoorganization')
#addSpecificNode('concept_personbelongstoorganization', 'abrahamlincoln')
