import datetime

from common.utils import base64_encode, base64_decode, get_last_modified_time

s = '''
{
  "type": "service_account",
  "project_id": "rever-150509",
  "private_key_id": "fcd996adbf9365cbf40f746aff7497db975275cb",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC7tn+Z9lTUa5Ic\nWRu4o2F9ojNnU38YfMjhz5oGefA4wDKA61vsVbu5RLcLW4aHHpIVBGsq81PZA2bP\nEkGcjO+A2y7/5Fao2XT3cd8W172MAk6YSWUAZea8Qm37TjpPytJ15DHPG6/sP4JW\nEqEPJRQXqD/MB8BfdMhFkvsApbQTdysD04Xyw5nY1rhzNodGhhehs5ZTHXZqb9Jr\n/GaUkhE51LqqayoxuzKsdClta9IEeFrSTjUqZrbVLIaLrcs63z2UND6tgp+LPoJS\n/3V6AtZf3kDad5F0CLRZaObIEAW/6Z3FF6pjkJbFvpQOGQj9dZhJhpDCa6BY33WO\ndijPnmOhAgMBAAECggEACH1UgA4YABNtZELTciJRgnEXihDIqr7aBvxwpuJU7vwi\nTJpYafPmrZZiAO1+xBWp3bF5d1PR8vqoZ60XspDdNCK1mXUmRjSviZFo2YugUvi3\nvUN0gfQZ32HRv4/mKz2tDw/nAMrrxe+JYYMuLZUrt3YwjHW9SZPqitr+5ySXZFnn\nayNBY/cuLKFB9QdP3GD5pkm8vzfiXKS4PWO0o1IEWA4REmIxNxp/sIe4lpUc0PUO\nmeAWsh3X9fkeTtfo+rHeZyjTixGXmAOKCrdvgNFhtKF3QmNERp45VWmYMOq7GGQM\nUFoho7T1/CRvBOV4a+SOOqQ5re0WtWopEvXpHJPdkQKBgQD3Qk5uNbW/HGd7Xyv0\nZiOaqrXtu0FxsPxNedwnmz2jFUnD+qkTt/C6KwAgsrN9XFMrM8lIaf/ehyHqw88+\nyHwpuf5RwE8NObXo+i1c9Su5dTDzwIAGviiTDrH8EVlo1XEWbRq8w8irv1p88mmx\nFm9n6wHsYpoEtcPm09fcNx+VEwKBgQDCWUy76IkBcjShEdvbdC10ZDcXaNHc8uFO\nLY2WdtrZwNYuRRGVJ5dxrhIqu5wNp6NS80ZYAU4i8bSPWMdqtdrdLYmkuwJIl72g\nD8wH6f0+QR5IRwGtGHd0PPzpHWnqYQtL4wE/WArLfbfExO1qcMkzjEE+9o3W1+QQ\n4LVwp/Ue+wKBgQCk4xradAbXg2Ge7ECrotFrexwHhTCHWLdoEzO1VdkswC2I+8d0\n1t05ySM0qvb3SnJMBSTdnZJ8GzGLFTlHbPsf+GCYs94Du9CLpQpLS1P1QSl744t8\n86KrLsmShx9QwcXAZtHFv+o6XklBuYayCXwRmK6XrMl5Cp2xeOQifsAY8QKBgQDB\np6fNabTzCvCkOp5fuxD79Evs+JZ9Wkrk0oFOF5qU9hC1RD9z52KBdRlHGXAzLwKQ\nSjaixJIuQbhQSf9TNmgikhigW3O4D/N0hakhjae5G7vn+1ERQNruqUK9qoB6ej5+\nXHFGxBzljZzK5gtIT6A8zBnLenP1S2RG6RICcmnOdwKBgQCDXk7MLV1a8p6G8WQr\nAMH/P6Fs2QQaM/RsPu/oeqZkijRuyP2dVtt/dvrHsBCvWo8Y2jEB+2RSAo8+tT4A\niyVUKTAjjzGO6pLgsc2JcPL9dSgWCP1FrDS1c2SMGdHgjjTV4PsN9ppW7/S3bRHs\nQCIBlSn6GJnCJleZzkpqGYoGEw==\n-----END PRIVATE KEY-----\n",
  "client_email": "rever-bi-staging@rever-150509.iam.gserviceaccount.com",
  "client_id": "111935822796164667397",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/rever-bi-staging%40rever-150509.iam.gserviceaccount.com"
}

'''

encoded = base64_encode(s)

print(encoded)
print('\n')
print(base64_decode('aGVsbG8gTMOqIFbhurduIEjhuqFuZw=='))

print('    d       a         '.strip())
print(get_last_modified_time('./local_test.py'))
print(get_last_modified_time('./local_test_ffsfsdsdsd.py'))

print(f'Now: {datetime.datetime.now().timestamp()}')
