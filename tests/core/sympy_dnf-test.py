# import unittest
#
# import sympy
# from sympy.abc import A, B, C
#
# if __name__ == '__main__':
#     unittest.main()
#
#
# class TestSympyDNF(unittest.TestCase):
#
#     def __init__(self, *args, **kwargs):
#         super(TestSympyDNF, self).__init__(*args, **kwargs)
#
#     def test_sympy_exp_to_DNF(self):
#         result = sympy.logic.boolalg.to_dnf(B & (A | C), simplify=False, force=False)
#
#         self.assertEqual(
#             (A & B) | (B & C),
#             result,
#             "Should match"
#         )
#
#     def test_python_exp_to_DNF(self):
#         result = sympy.logic.boolalg.to_dnf((2 > 3) & ((4 == 4) | (7 > 0)),
#                                             simplify=False, force=False)
#
#         self.assertEqual(
#             ((4 == 4) & (2 > 3) | ((2 > 3) & (7 > 0))),
#             result,
#             "Should match"
#         )
#
#     def test_arrow_exp_to_DNF(self):
#         import pyarrow.dataset as ds
#
#         dataset = ds.dataset( "../../../../../data/input/diabetes/parquet/nopart", format="parquet")
#         result = sympy.logic.boolalg.to_dnf((ds.field("Pregnancies") >= 2) & ((ds.field("Pregnancies") == 3) | (ds.field("Pregnancies") == 4)),
#                                             simplify=False, force=False)
#
#         self.assertEqual(
#             ((ds.field("Pregnancies") >= 2) & (ds.field("Pregnancies") == 3)) | ((ds.field("Pregnancies") == 3) & (ds.field("Pregnancies") == 4)),
#             result,
#             "Should match"
#         )
