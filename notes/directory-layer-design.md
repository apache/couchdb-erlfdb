Directory Layer Schema-ish
==========================


`'\x16...'` to `'\x1D...'` - Keys that represent contents of the directory
    tree. This is basically "all possible positive integers". The prefix
    of each key is a directory node down below.


`'\xFE'` - Prefix for all keys related to the directory tree node
    hierarchy.


`'\xFE\x01version\x00'` - Version of the directory tree formatted as:
    <<V1:32/little-unsigned, V2:32/little-unsigned, V3:32/little-unsigned>>


`'\xFE\x01\xFE\x00'` - the root node of the tree


`'\xFE\x01\xFE\x00\x01layer\x00'` - the "layer" of the given node. This
    is set at node creation time and never mutated by the directory layer.
    If a layer is provided when opening nodes it checks to see that the
    layer matches nodes that are read. When there's a mismatch an error
    is thrown. I believe the purpose of this is so that users of the
    DirectoryLayer can assert a primitive "ownership" over part of the
    directory tree with some guarantee that they won't accidentally trample
    over each other. However, if "layer" is the binary string "partition"
    then this becomes a whole new thing which I'll discuss below.


`'\xFE\x01\xFE\x00\x16' - The prefix for all child nodes of the root node. The
    pattern here is basically `'\xFE'` + erlfdb_tupe:pack({node.key(), 0})
    although its using erlfdb_subspaces under the hood. I may ixnay subspaces
    in the directory layer because the syntax really isn't all that useful
    in Erlang.


`'\xFE\x01\xFE\x00\x16\x02foo\x00'` -> `'\x17\x05'` - A child node named "foo"
    which has a node id of `\x17\x05`.


`'\xFE\x17\x05\x16\x02bar\x00'` -> `'\x17\x19'` - A child of "foo" named
    "bar" with node id `'\x17\x19'`. The tree is made by recursively
    following these paths.


`'\xFE\x17\x19\x01layer\x00'` -> `'partition'` A node id has its layer value
    set to partition which means that this node is "DirectoryPartition"
    which basically means its node id is pasted onto the front of
    every subtree node. This can be useful to do a range scan against an
    entire subtree. However, directories can not be moved across partition
    boundaries (because that would require changing all of their key
    prefixes). Also, it makes key lengths slightly longer by the length
    of the short node id string.

    Partitions are roughly equivalent to a nested directory tree re-rooted
    outside the main directory tree. All of the contents and node keys
    share the same prefix based on the node id of the partition.


Some Names
==========

A warning to future readers, this list of names is used fairly consistently
by the `erlfdb` DirectoryLayer, however they are not all used in the
Python implementation. So any comparison between the two requires some
fairly decent knowledge of how things work. This is mostly because the
Python implementation doesn't actually bother naming things and instead has
a number of long function invocations that gloss over where we might need
to assign something to a variable in Erlang.

  * directory name = human readable string
  * node_name = shortened binary
  * root version = node_prefix + {"version"}
  * root node = node_prefix + {node_prefix}
  * partition id = node_name + 16#FE
  * partition version = node_name + {"version"}
  * partition node = partition_id + {partition_id}
  * node_id = node_prefix + {node_name}
  * node_layer_id = node_prefix + {node_name, "layer"}
  * node_entry_id = node_prefix + {node_name, SUBDIRS, directory name}



Algorithms
==========

These are all based on the assumption that my schema is at least relatively
close to reality and also by staring at the Python implementation. However,
the Python implementation makes extensive use of OO inheritance as well
as syntactical sugar via magic methods. These algorithms are my general
reinterpretation of the Python approach in Erlang terms.

Some common assumptions here:

  * Path is a tuple of UTF-8 encoded binaries.


Find Node at Path
-----------------

  * Get the root node (i.e., `\xFE\x01\xFE\x00`) (though this will
    obviously be parameterizable).
  * curr_node = RootNode
  * For part in path:
    - node_id = erlfdb:get(Tx, Root + to_string(Path))
    - if node_id == not_found: return not_found
    - curr_node = `'\xFE' + node_id`
  * return curr_node


Open Directory at Path
----------------------

  * Find node at path
  * If not found, return not_found or throw an error or w/e
  * Read layer value
  * Optionally pre-fetch child keys. Would be more efficient as
    a single range scan, but super wide trees would be ungood
  * if layer != "partition": Return value representing this directory
    - root reference
    - path referecne
    - layer
    - children
  * else: Return record representing this partition
    - previous root
    - new root
    - path to partition root


Create New Directory at Path
----------------------------

  * Find directory at Path[:-1]
  * If directory[Path[-1]] exists: return exists
  * Allocate new node id via HCA
  * Write `'\xFE' + node_id + '\x01layer\x00' = whatever was passed
  * return samesies as for open path


Remove A Directory at Path
--------------------------

  * Find node at Path
  * del_children(node_id):
  *    for name, child_id in children(node_id):
  *        del_children(child_id)
  *        clear_range for all content related to child_id
  *    clear_range for this node's layer and children nodes
  * delete node_id from Path[:-1]


Move a Directory from PathA to PathB
------------------------------------

  * Bunch of error checking and partition handling
  * Insert PathA node_id to PathB[:-1]'s list of children
  * Then remove PathA's node_id from PathA[:-1]


List Directory at Path
----------------------

  * Find Directory at Path
  * Return children list. Mebbe after a range read if they
    haven't been pre-loaded.


Directory Exists
----------------

  * Find directory, see if it throws an error


Tests
-----

  * Opening a directory creates parent directories?
  * Creating parent directories does not set a label?
  * Opening nested directories does not check intermediate labels?



