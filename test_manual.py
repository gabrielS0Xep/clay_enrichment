#!/usr/bin/env python3
"""
Script de testing manual para la API
Ejecuta tests básicos sin necesidad de servicios reales de Google Cloud
"""

import requests
import json
from datetime import datetime

# Configuración para testing local
BASE_URL = "http://localhost:8080"

def test_health_check():
    """Test del endpoint de health check"""
    print("🔍 Testing health check...")
    try:
        response = requests.get(f"{BASE_URL}/status")
        print(f"✅ Status: {response.status_code}")
        print(f"✅ Response: {response.json()}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_post_contacts():
    """Test del endpoint POST /contacts"""
    print("\n🔍 Testing POST /contacts...")
    
    # Datos de prueba
    test_data = {
        "biz_name": "Test Company Inc",
        "biz_identifier": "TEST123456",
        "full_name": "John Doe Smith",
        "role": "Senior Software Engineer",
        "phone_number": "56912345678",
        "cat": "Technology",
        "web_linkedin_url": "https://linkedin.com/in/johndoe",
        "phone_exists": True,
        "src_scraped_name": "Manual Test"
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/contacts",
            json=test_data,
            headers={"Content-Type": "application/json"}
        )
        
        print(f"✅ Status: {response.status_code}")
        print(f"✅ Response: {json.dumps(response.json(), indent=2)}")
        
        if response.status_code == 200:
            print("✅ Test exitoso!")
            return True
        else:
            print("❌ Test falló!")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_post_contacts_invalid_json():
    """Test con JSON inválido"""
    print("\n🔍 Testing POST /contacts with invalid JSON...")
    
    try:
        response = requests.post(
            f"{BASE_URL}/contacts",
            data="invalid json data",
            headers={"Content-Type": "application/json"}
        )
        
        print(f"✅ Status: {response.status_code}")
        print(f"✅ Response: {json.dumps(response.json(), indent=2)}")
        
        if response.status_code == 400:
            print("✅ Test exitoso (esperado error 400)!")
            return True
        else:
            print("❌ Test falló!")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_get_companies():
    """Test del endpoint GET /companies"""
    print("\n🔍 Testing GET /companies...")
    
    try:
        response = requests.get(f"{BASE_URL}/companies?batch_size=5")
        print(f"✅ Status: {response.status_code}")
        print(f"✅ Response: {json.dumps(response.json(), indent=2)}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_datetime_format():
    """Test del formato de datetime para Avro"""
    print("\n🔍 Testing datetime format for Avro...")
    
    # Simular el formato que se usa en el código
    timestamp_micros = int(datetime.now().timestamp() * 1000000)
    
    print(f"✅ Timestamp micros: {timestamp_micros}")
    print(f"✅ Type: {type(timestamp_micros)}")
    print(f"✅ Is int: {isinstance(timestamp_micros, int)}")
    
    # Verificar que es compatible con Avro timestamp-micros
    if isinstance(timestamp_micros, int) and timestamp_micros > 0:
        print("✅ Formato compatible con Avro timestamp-micros!")
        return True
    else:
        print("❌ Formato no compatible!")
        return False

def main():
    """Ejecutar todos los tests"""
    print("🚀 Iniciando tests manuales...")
    print("=" * 50)
    
    tests = [
        ("Health Check", test_health_check),
        ("POST /contacts", test_post_contacts),
        ("POST /contacts (invalid JSON)", test_post_contacts_invalid_json),
        ("GET /companies", test_get_companies),
        ("DateTime Format", test_datetime_format)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n📋 Running: {test_name}")
        print("-" * 30)
        result = test_func()
        results.append((test_name, result))
    
    # Resumen final
    print("\n" + "=" * 50)
    print("📊 RESUMEN DE TESTS")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASÓ" if result else "❌ FALLÓ"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n🎯 Resultado: {passed}/{total} tests pasaron")
    
    if passed == total:
        print("🎉 ¡Todos los tests pasaron!")
    else:
        print("⚠️  Algunos tests fallaron. Revisa los errores arriba.")

if __name__ == "__main__":
    main()
