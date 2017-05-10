import heapq
#from utility import *


class node:
	# The node of a tree
	def __init__(self, value):
		self.childList = []
		self.edgeList = []
		self.parent = None
		self.value = value

	def getValue(self):
		return self.value

	def addChild(self, child, weight):
		self.childList.append(child)
		self.edgeList.append(weight)

	def addChildren(self, childList, edgeList):
		self.childList.extend(childList)
		self.edgeList.extend(edgeList)
	
	def __repr__(self, level=0):
		ret = "\t"*level+repr(self.value)+"\n"
		for child in self.childList:
			ret += child.__repr__(level+1)
		return ret


class tree:
	def __init__(self, root = None):
		self.root = root

	def printTree(self):
		print(self.root)

	def addChild(self, parent, child, weight):
		parent.childList.append(child)
		parent.edgeList.append(weight)

	def addChildren(self, parent, childList, edgeList):
		parent.childList.extend(childList)
		parent.edgeList.extend(edgeList)

"""
class iterator:
	def __init__(self):
		self.cur = None

	def run:
		initState = getinitState()
		root = node(initState)
		self.cur = root
		searchSpace = tree(root)
		while(1):
			children = getnextStates(cur.value)
			edges = utility(children)
			cur.addChildren(children, edges)
"""


