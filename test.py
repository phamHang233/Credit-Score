my_dict= {
 "venus": {
  "0xe14f5cc9b3885e7454a3f220710bc693d5f02bbf": {
   "borrow_amount": 238.29000000000002,
   "deposit_with_lth": 0
  }},
"aave": {
  "0x1210c3425cf38a442b9629a4edfc92a1ee0bed62": {
   "borrow_amount": 0,
   "deposit_with_lth": 1.1823247147742078
  },
  "0xef22a550eb632055e58c2f20487b1a9a12a66d29": {
   "borrow_amount": 48.0737,
   "deposit_with_lth": 87.99957102581999
  }}}
unique_addresses= set()
for key, sub_dict in my_dict.items():
    for address in sub_dict.keys():
        unique_addresses.add(address)
print(unique_addresses)