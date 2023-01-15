from custom import KubeShareFunc, KubeData


kube_share_dict = {}

ksf = KubeShareFunc()
kb = KubeData(kube_share_func=ksf, kube_share_dict=kube_share_dict)


print(kube_share_dict)
kb.update_all_list()
print(kube_share_dict)

print(kb.get_pod_list())
# print(ksf.get_deserialize_pod_list())

# print(ksf.get_serialize_pod_list().data)
