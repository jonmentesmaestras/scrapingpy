"""
Facebook Authentication Module
Maneja login, persistencia de cookies y verificación de sesión.
"""

import os
import json
import time
from pathlib import Path
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

# Cargar variables de entorno
load_dotenv()

# Ruta para guardar cookies
COOKIES_DIR = Path(__file__).parent / "cookies"
COOKIES_FILE = COOKIES_DIR / "fb_cookies.json"


class FacebookAuth:
    """
    Clase para manejar autenticación de Facebook con persistencia de cookies.
    """
    
    def __init__(self, driver=None):
        self.email = os.getenv("FB_EMAIL")
        self.password = os.getenv("FB_PASSWORD")
        self.driver = driver
        self._ensure_cookies_dir()
    
    def _ensure_cookies_dir(self):
        """Asegura que el directorio de cookies exista."""
        COOKIES_DIR.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def create_driver_for_auth():
        """
        Crea un driver de Chrome configurado para autenticación.
        NO usa headless para permitir verificación humana.
        """
        options = webdriver.ChromeOptions()
        options.page_load_strategy = 'eager'
        options.add_argument("--start-maximized")
        options.add_argument('--lang=en')
        options.add_argument("--disable-notifications")
        return webdriver.Chrome(options=options)
    
    @staticmethod
    def create_driver_for_scraping():
        """
        Crea un driver de Chrome configurado para scraping.
        Puede usar headless si se desea.
        """
        options = webdriver.ChromeOptions()
        options.page_load_strategy = 'eager'
        options.add_argument("--start-maximized")
        # options.add_argument("--headless")  # Descomentar para modo headless
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-accelerated-2d-canvas")
        options.add_argument("--disable-accelerated-video-decode")
        options.add_argument("--use-gl=swiftshader")
        options.add_argument('--lang=en')
        options.add_argument("--disable-notifications")
        return webdriver.Chrome(options=options)
    
    def save_cookies(self, driver=None):
        """
        Guarda las cookies del driver actual en un archivo JSON.
        """
        driver = driver or self.driver
        if not driver:
            print("❌ No hay driver disponible para guardar cookies.")
            return False
        
        try:
            cookies = driver.get_cookies()
            with open(COOKIES_FILE, 'w') as f:
                json.dump(cookies, f, indent=2)
            print(f"🍪 Cookies guardadas en: {COOKIES_FILE}")
            return True
        except Exception as e:
            print(f"❌ Error al guardar cookies: {e}")
            return False
    
    def load_cookies(self, driver=None):
        """
        Carga las cookies desde el archivo JSON al driver.
        """
        driver = driver or self.driver
        if not driver:
            print("❌ No hay driver disponible para cargar cookies.")
            return False
        
        if not COOKIES_FILE.exists():
            print("ℹ️ No hay cookies guardadas previamente.")
            return False
        
        try:
            # Primero navegar a Facebook para poder agregar cookies del dominio
            driver.get("https://www.facebook.com/")
            time.sleep(2)
            
            with open(COOKIES_FILE, 'r') as f:
                cookies = json.load(f)
            
            for cookie in cookies:
                # Algunos campos pueden causar problemas, los removemos
                cookie.pop('sameSite', None)
                cookie.pop('expiry', None)
                try:
                    driver.add_cookie(cookie)
                except Exception:
                    pass  # Ignorar cookies que no se puedan agregar
            
            print(f"🍪 Cookies cargadas desde: {COOKIES_FILE}")
            return True
        except Exception as e:
            print(f"❌ Error al cargar cookies: {e}")
            return False
    
    def is_logged_in(self, driver=None):
        """
        Verifica si el usuario está logueado en Facebook.
        """
        driver = driver or self.driver
        if not driver:
            return False
        
        try:
            # Navegar a Facebook y verificar elementos que solo aparecen logueado
            driver.get("https://www.facebook.com/")
            time.sleep(3)
            
            # Lista de selectores que indican sesión activa
            logged_in_selectors = [
                (By.CSS_SELECTOR, "[aria-label='Facebook']"),
                (By.CSS_SELECTOR, "[aria-label='Home']"),
                (By.CSS_SELECTOR, "[aria-label='Your profile']"),
                (By.CSS_SELECTOR, "[role='navigation']"),
                (By.XPATH, "//a[contains(@href, '/me/')]"),
            ]
            
            for by, selector in logged_in_selectors:
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    print("✅ Sesión de Facebook activa.")
                    return True
                except TimeoutException:
                    continue
            
            # Si llegamos aquí, verificar si hay campo de login (no logueado)
            try:
                driver.find_element(By.ID, "email")
                print("ℹ️ No hay sesión activa (campo de login encontrado).")
                return False
            except:
                pass
            
            return False
            
        except Exception as e:
            print(f"⚠️ Error verificando sesión: {e}")
            return False
    
    def _wait_for_element(self, by, value, timeout=30, retries=3):
        """Espera por un elemento con reintentos automáticos."""
        for attempt in range(retries):
            try:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((by, value))
                )
                return element
            except TimeoutException:
                if attempt < retries - 1:
                    print(f"⏳ Intento {attempt + 1}/{retries} fallido. Reintentando...")
                    time.sleep(2)
                else:
                    raise
    
    def _wait_and_click(self, by, value, timeout=30, retries=3):
        """Espera por un elemento y hace clic con reintentos automáticos."""
        for attempt in range(retries):
            try:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.element_to_be_clickable((by, value))
                )
                element.click()
                return True
            except TimeoutException:
                if attempt < retries - 1:
                    print(f"⏳ Intento {attempt + 1}/{retries} fallido. Reintentando...")
                    time.sleep(2)
                else:
                    raise
    
    def perform_login(self, driver=None):
        """
        Realiza el proceso de login interactivo.
        Retorna True si el login fue exitoso.
        """
        self.driver = driver or self.driver
        if not self.driver:
            print("❌ No hay driver disponible para login.")
            return False
        
        try:
            print("🔵 Navegando a Facebook...")
            self.driver.get("https://www.facebook.com/")
            time.sleep(3)
            
            # Manejo de Cookies popup
            try:
                cookie_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@title, 'Allow')]"))
                )
                cookie_button.click()
                print("🍪 Cookies del navegador aceptadas.")
                time.sleep(1)
            except TimeoutException:
                print("ℹ️ No apareció banner de cookies, continuando...")
            
            # Email
            print("📧 Introduciendo email...")
            email_field = self._wait_for_element(By.ID, "email", timeout=30, retries=3)
            email_field.clear()
            time.sleep(0.5)
            email_field.send_keys(self.email)
            time.sleep(1)
            
            # Password
            print("🔑 Introduciendo contraseña...")
            pass_field = self._wait_for_element(By.ID, "pass", timeout=30, retries=3)
            pass_field.clear()
            time.sleep(0.5)
            pass_field.send_keys(self.password)
            time.sleep(1)
            
            # Click Login
            print("🚀 Iniciando sesión...")
            self._wait_and_click(By.NAME, "login", timeout=30, retries=3)
            
            # Espera para verificación humana (si es necesaria)
            print("⏳ Esperando verificación (si Facebook la solicita)...")
            print("   👆 Si aparece un captcha o verificación, complétalo manualmente.")
            
            # Esperar hasta 120 segundos para verificación humana
            login_success = False
            max_wait = 120
            check_interval = 5
            elapsed = 0
            
            while elapsed < max_wait:
                time.sleep(check_interval)
                elapsed += check_interval
                
                # Verificar si ya estamos logueados
                try:
                    selectors = [
                        (By.CSS_SELECTOR, "[aria-label='Facebook']"),
                        (By.CSS_SELECTOR, "[aria-label='Home']"),
                        (By.CSS_SELECTOR, "[role='navigation']"),
                    ]
                    
                    for by, selector in selectors:
                        try:
                            self.driver.find_element(by, selector)
                            login_success = True
                            break
                        except:
                            continue
                    
                    if login_success:
                        break
                        
                except Exception:
                    pass
                
                print(f"   ⏳ Esperando... ({elapsed}s / {max_wait}s)")
            
            if login_success:
                print("✅ Login exitoso!")
                # Guardar cookies inmediatamente
                self.save_cookies(self.driver)
                return True
            else:
                print("❌ Login no completado en el tiempo esperado.")
                return False
                
        except TimeoutException:
            print("❌ Error: Tiempo de espera agotado.")
            return False
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            return False
    
    def get_authenticated_driver(self):
        """
        Retorna un driver autenticado.
        1. Intenta cargar cookies existentes
        2. Si las cookies son válidas, retorna el driver
        3. Si no, hace login interactivo y guarda las cookies
        """
        # Crear driver para autenticación
        driver = self.create_driver_for_auth()
        self.driver = driver
        
        print("🔐 Iniciando proceso de autenticación de Facebook...")
        
        # Intentar cargar cookies existentes
        if COOKIES_FILE.exists():
            print("📂 Encontradas cookies guardadas, intentando cargar...")
            if self.load_cookies(driver):
                # Verificar si las cookies son válidas
                driver.refresh()
                time.sleep(3)
                
                if self.is_logged_in(driver):
                    print("✅ Autenticación exitosa con cookies guardadas!")
                    return driver
                else:
                    print("⚠️ Cookies expiradas o inválidas. Se requiere nuevo login.")
        
        # Si llegamos aquí, necesitamos hacer login
        print("🔑 Se requiere login interactivo...")
        
        if not self.email or not self.password:
            print("❌ Error: Credenciales no configuradas en .env (FB_EMAIL, FB_PASSWORD)")
            driver.quit()
            return None
        
        if self.perform_login(driver):
            return driver
        else:
            driver.quit()
            return None
    
    def open_new_tab_for_scraping(self, driver, url):
        """
        Abre una nueva pestaña y navega a la URL especificada.
        Retorna el handle de la nueva pestaña.
        """
        try:
            # Guardar handle original
            original_handle = driver.current_window_handle
            
            # Abrir nueva pestaña
            driver.execute_script("window.open('');")
            time.sleep(1)
            
            # Cambiar a la nueva pestaña
            new_handle = [h for h in driver.window_handles if h != original_handle][-1]
            driver.switch_to.window(new_handle)
            
            # Navegar a la URL
            print(f"🌐 Navegando a: {url}")
            driver.get(url)
            time.sleep(3)
            
            return new_handle
            
        except Exception as e:
            print(f"❌ Error abriendo nueva pestaña: {e}")
            return None


def get_authenticated_driver_for_scraping():
    """
    Función de utilidad para obtener un driver autenticado listo para scraping.
    """
    auth = FacebookAuth()
    driver = auth.get_authenticated_driver()
    
    if driver:
        # Bloquear recursos pesados para scraping
        try:
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Network.setBlockedURLs", {
                "urls": ["*.mp4", "*.png", "*.jpg", "*.jpeg", "*.gif", "*video*"]
            })
        except Exception as e:
            print(f"⚠️ No se pudieron bloquear recursos: {e}")
    
    return driver


# Test standalone
if __name__ == "__main__":
    print("🧪 Test de Facebook Auth")
    auth = FacebookAuth()
    driver = auth.get_authenticated_driver()
    
    if driver:
        print("✅ Driver autenticado obtenido exitosamente!")
        print("📌 Manteniendo navegador abierto 30 segundos para verificación...")
        time.sleep(30)
        driver.quit()
    else:
        print("❌ No se pudo obtener driver autenticado.")
