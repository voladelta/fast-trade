import unittest
from fast_trade.utils import parse_logic_expr


class TestParseLogicExpression(unittest.TestCase):
    def test_basic_cases(self):
        """Test basic parsing functionality"""
        self.assertEqual(parse_logic_expr("rsi < 30"), ["rsi", "<", 30])
        self.assertEqual(parse_logic_expr("bbands_bbands_bb_lower > close"), ["bbands_bbands_bb_lower", ">", "close"])

    def test_operators(self):
        """Test all supported operators"""
        self.assertEqual(parse_logic_expr("rsi > 70"), ["rsi", ">", 70])
        self.assertEqual(parse_logic_expr("rsi = 50"), ["rsi", "=", 50])
        self.assertEqual(parse_logic_expr("rsi >= 30"), ["rsi", ">=", 30])
        self.assertEqual(parse_logic_expr("rsi <= 70"), ["rsi", "<=", 70])

    def test_numeric_values(self):
        """Test numeric value parsing"""
        self.assertEqual(parse_logic_expr("rsi < 30.5"), ["rsi", "<", 30.5])
        self.assertEqual(parse_logic_expr("rsi > -10"), ["rsi", ">", -10])
        self.assertEqual(parse_logic_expr("rsi < -5.5"), ["rsi", "<", -5.5])

    def test_field_names_with_dots(self):
        """Test field names containing dots"""
        self.assertEqual(parse_logic_expr("bbands.upper > close"), ["bbands.upper", ">", "close"])
        self.assertEqual(parse_logic_expr("bbands.lower < close"), ["bbands.lower", "<", "close"])

    def test_whitespace_handling(self):
        """Test that extra whitespace is handled correctly"""
        self.assertEqual(parse_logic_expr("rsi  <  30"), ["rsi", "<", 30])
        self.assertEqual(parse_logic_expr("  rsi > 70  "), ["rsi", ">", 70])

    def test_error_cases(self):
        """Test that invalid expressions raise ValueError"""
        with self.assertRaises(ValueError):
            parse_logic_expr("invalid format")

        with self.assertRaises(ValueError):
            parse_logic_expr("rsi invalid_op 30")

        with self.assertRaises(ValueError):
            parse_logic_expr("rsi <")  # Missing value

        with self.assertRaises(ValueError):
            parse_logic_expr("rsi")  # Missing operator and value

    def test_field_name_validation(self):
        """Test that field names must start with letter or underscore"""
        self.assertEqual(parse_logic_expr("_rsi < 30"), ["_rsi", "<", 30])
        self.assertEqual(parse_logic_expr("rsi_2 < 30"), ["rsi_2", "<", 30])

        # These should work (field names can contain dots, letters, numbers, underscores)
        self.assertEqual(parse_logic_expr("field.name_123 > value"), ["field.name_123", ">", "value"])


if __name__ == "__main__":
    unittest.main()
