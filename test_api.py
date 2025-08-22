import unittest
import json
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Agregar el directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

class TestPubSubService(unittest.TestCase):
    """Test para PubSubService con mocks"""
    
    def setUp(self):
        """Configurar mocks antes de cada test"""
        self.mock_publisher = Mock()
        self.mock_future = Mock()
        
    @patch('src.pub_sub_services.pubsub_v1.PublisherClient')
    def test_publish_message_success(self, mock_publisher_client):
        """Test publicación exitosa de mensaje"""
        # Configurar mocks
        mock_publisher_client.return_value = self.mock_publisher
        self.mock_publisher.publish.return_value = self.mock_future
        self.mock_future.result.return_value = "test-message-id-123"
        
        # Importar después de configurar mocks
        from src.pub_sub_services import PubSubService
        
        # Crear instancia del servicio
        service = PubSubService("test-project", "test-topic")
        
        # Datos de prueba
        test_data = {
            "biz_name": "Test Company",
            "full_name": "John Doe",
            "phone_number": "1234567890",
            "src_scraped_dt": int(datetime.now().timestamp() * 1000000)
        }
        
        # Ejecutar método
        result = service.publish_message(test_data)
        
        # Verificaciones
        self.assertTrue(result)
        self.mock_publisher.publish.assert_called_once()
        self.mock_future.result.assert_called_once()
        
        # Verificar que se llamó con datos JSON
        call_args = self.mock_publisher.publish.call_args
        self.assertIn("test-topic", call_args[0])
        
    @patch('src.pub_sub_services.pubsub_v1.PublisherClient')
    def test_publish_message_failure(self, mock_publisher_client):
        """Test publicación fallida de mensaje"""
        # Configurar mocks para fallar
        mock_publisher_client.return_value = self.mock_publisher
        self.mock_publisher.publish.return_value = self.mock_future
        self.mock_future.result.side_effect = Exception("Test error")
        
        # Importar después de configurar mocks
        from src.pub_sub_services import PubSubService
        
        # Crear instancia del servicio
        service = PubSubService("test-project", "test-topic")
        
        # Datos de prueba
        test_data = {"test": "data"}
        
        # Verificar que se lanza excepción
        with self.assertRaises(Exception):
            service.publish_message(test_data)


class TestMainAPI(unittest.TestCase):
    """Test para la API principal con mocks"""
    
    def setUp(self):
        """Configurar Flask app para testing"""
        from src.main import app
        self.app = app.test_client()
        self.app.testing = True
        
    @patch('src.main.get_services')
    def test_post_contacts_success(self, mock_get_services):
        """Test POST /contacts exitoso"""
        # Mock del servicio PubSub
        mock_pubsub_service = Mock()
        mock_pubsub_service.publish_message.return_value = True
        mock_get_services.return_value = (None, mock_pubsub_service)
        
        # Datos de prueba
        test_data = {
            "biz_name": "Test Company",
            "biz_identifier": "TEST123",
            "full_name": "John Doe",
            "role": "Manager",
            "phone_number": "1234567890",
            "cat": "Technology",
            "web_linkedin_url": "https://linkedin.com/in/johndoe",
            "phone_exists": True,
            "src_scraped_name": "Test Source"
        }
        
        # Hacer request
        response = self.app.post('/contacts', 
                               data=json.dumps(test_data),
                               content_type='application/json')
        
        # Verificaciones
        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.data)
        self.assertTrue(response_data['success'])
        self.assertIn('message', response_data)
        
    def test_post_contacts_invalid_json(self):
        """Test POST /contacts con JSON inválido"""
        # Hacer request sin JSON válido
        response = self.app.post('/contacts', 
                               data="invalid json",
                               content_type='application/json')
        
        # Verificaciones
        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.data)
        self.assertFalse(response_data['success'])
        
    @patch('src.main.get_services')
    def test_post_contacts_pubsub_error(self, mock_get_services):
        """Test POST /contacts cuando PubSub falla"""
        # Mock del servicio PubSub que falla
        mock_pubsub_service = Mock()
        mock_pubsub_service.publish_message.side_effect = Exception("PubSub error")
        mock_get_services.return_value = (None, mock_pubsub_service)
        
        # Datos de prueba
        test_data = {
            "biz_name": "Test Company",
            "full_name": "John Doe",
            "phone_number": "1234567890"
        }
        
        # Hacer request
        response = self.app.post('/contacts', 
                               data=json.dumps(test_data),
                               content_type='application/json')
        
        # Verificaciones
        self.assertEqual(response.status_code, 500)
        response_data = json.loads(response.data)
        self.assertFalse(response_data['success'])


class TestDateTimeFormat(unittest.TestCase):
    """Test para el formato de datetime compatible con Avro"""
    
    def test_datetime_to_avro_timestamp_micros(self):
        """Test conversión de datetime a timestamp-micros de Avro"""
        from datetime import datetime
        
        # Crear datetime de prueba
        test_datetime = datetime(2024, 1, 15, 10, 30, 45, 123456)
        
        # Convertir a timestamp-micros
        timestamp_micros = int(test_datetime.timestamp() * 1000000)
        
        # Verificaciones
        self.assertIsInstance(timestamp_micros, int)
        self.assertGreater(timestamp_micros, 0)
        
        # Verificar que es compatible con Avro timestamp-micros
        # timestamp-micros debe ser un long (int en Python)
        self.assertTrue(isinstance(timestamp_micros, int))


def run_tests():
    """Ejecutar todos los tests"""
    # Crear test suite
    test_suite = unittest.TestSuite()
    
    # Agregar tests
    test_suite.addTest(unittest.makeSuite(TestPubSubService))
    test_suite.addTest(unittest.makeSuite(TestMainAPI))
    test_suite.addTest(unittest.makeSuite(TestDateTimeFormat))
    
    # Ejecutar tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
