import tests._bootstrap
import unittest

from app.services.admin_service import validate_custom_plan_days


class AdminServiceTests(unittest.TestCase):
    def test_validate_custom_plan_days(self):
        self.assertEqual(validate_custom_plan_days(30), (True, None))
        self.assertEqual(validate_custom_plan_days(0), (False, 'non_positive'))
        self.assertEqual(validate_custom_plan_days(3651), (False, 'too_high'))


if __name__ == '__main__':
    unittest.main()
