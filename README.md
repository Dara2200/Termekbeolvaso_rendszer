# Okos Termékbeolvasó Rendszer

Ez a szakdolgozati projekt egy Raspberry Pi-alapú beolvasó rendszert valósít meg, amely képes egy termék méretének, súlyának meghatározására, valamint kép- és szenzoradatok továbbítására Apache Kafka segítségével.

### Használt technológiák

- Raspberry Pi 4B
- Python 3
- Apache Kafka (Confluent)
- OpenCV
- Ultrahangos szenzor (HC-SR04)
- HX711 mérlegmodul
- Nema 17 + TMC2209/A4988 motorvezérlők
- 3D nyomtatás (Fusion 360 modellek)



## Könyvtárak telepítése Raspberry pi 4 / Windowsra

```bash
pip install -r requirements_raspb.txt
pip install -r requirements_win.txt
```
A server_otthon.properties filet el kell helyezni a kafka kraft mappájában.



