#
# import json
#
# class A:
#     def __init__(self, i):
#         self.i = i
#
#
# class B(A):
#     def __init__(self, j,x):
#         A.__init__(self,x)
#         self.j = j
#
#
# b = B(5,50)
# print(b.j)
# print(b.i)
#
#
#
# class Student:
#     def __init__(self, roll_no, name, batch):
#         self.roll_no = roll_no
#         self.name = name
#         self.batch = batch
#
#
# s1 = Student("85", "Swapnil", "IMT")
#
# jsonstr1 = json.dumps(s1.__dict__)
# print(jsonstr1)
#
#
#
# import json
#
# class Identity:
#     def __init__(self):
#         self.name="abc name"
#         self.first="abc first"
#         self.addr=Addr()
#     def reprJSON(self):
#         return dict(name=self.name, firstname=self.first, address=self.addr)
#
# class Addr:
#     def __init__(self):
#         self.street="sesame street"
#         self.zip="13000"
#     def reprJSON(self):
#         return dict(street=self.street, zip=self.zip)
#
# class Doc:
#     def __init__(self):
#         self.identity=Identity()
#         self.data="all data"
#     def reprJSON(self):
#         return dict(id=self.identity, data=self.data)
#
# class ComplexEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if hasattr(obj,'reprJSON'):
#             return obj.reprJSON()
#         else:
#             return json.JSONEncoder.default(self, obj)
#
# doc=Doc()
# print("-------------------------------")
# print ("Str representation")
# print (doc.reprJSON())
# print ("Full JSON")
# print (json.dumps(doc.reprJSON(), cls=ComplexEncoder))
# print ("Partial JSON")
# print (json.dumps(doc.identity.addr.reprJSON(), cls=ComplexEncoder))
# print("-------------------------------")
#
from com.db.fw.etl.core.common.DeltaStatLogger import IOService
import json
store =  IOService()
pipeline_id ,pipeline_name ,node_defination = store.get_pipeline_metadata("my_dummy_graph","sample")
nodes = json.loads(node_defination)
print(nodes)


