import os
import time
from dotenv import load_dotenv

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# Cargar variables de entorno
load_dotenv()

class FacebookBot:
    def __init__(self):
        self.email = os.getenv("FB_EMAIL")
        self.password = os.getenv("FB_PASSWORD")
        self.driver = self._setup_driver()
        self.wait = WebDriverWait(self.driver, 30) # Espera máxima de 30 segundos

    def _setup_driver(self):
        """
        Configura el driver de Chrome con las opciones solicitadas.
        """
        options = webdriver.ChromeOptions()
        
        # Estrategia 'eager': El driver espera a que el DOM esté cargado, 
        # pero no espera a imágenes o estilos pesados. Ideal para velocidad.
        options.page_load_strategy = 'eager' 
        
        options.add_argument("--start-maximized")
        options.add_argument('--lang=en')
        
        # Opciones adicionales recomendadas para evitar popups molestos de Facebook
        options.add_argument("--disable-notifications") 
        
        # Inicializar el driver
        driver = webdriver.Chrome(options=options)
        return driver

    def _wait_for_element(self, by, value, timeout=30, retries=3):
        """
        Espera por un elemento con reintentos automáticos.
        """
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
        """
        Espera por un elemento y hace clic con reintentos automáticos.
        """
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

    def login(self):
        """
        Realiza el proceso de inicio de sesión en Facebook.
        """
        try:
            print("🔵 Navegando a Facebook...")
            self.driver.get("https://www.facebook.com/")
            time.sleep(3)  # Espera adicional para que la página cargue completamente

            # 1. Manejo de Cookies (Opcional, depende de la región, pero es buena práctica)
            # A veces aparece un modal de "Allow essential cookies". 
            # Si no aparece, el bloque try/except lo ignorará y continuará.
            try:
                cookie_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@title, 'Allow')]"))
                )
                cookie_button.click()
                print("🍪 Cookies aceptadas.")
                time.sleep(1)
            except TimeoutException:
                print("ℹ️ No apareció banner de cookies, continuando...")
                pass # No apareció el banner de cookies, continuamos.

            # 2. Localizar e interactuar con el campo de Email
            print("📧 Introduciendo email...")
            email_field = self._wait_for_element(By.ID, "email", timeout=30, retries=3)
            email_field.clear()
            time.sleep(0.5)
            email_field.send_keys(self.email)
            time.sleep(1)

            # 3. Localizar e interactuar con el campo de Password
            print("🔑 Introduciendo contraseña...")
            pass_field = self._wait_for_element(By.ID, "pass", timeout=30, retries=3)
            pass_field.clear()
            time.sleep(0.5)
            pass_field.send_keys(self.password)
            time.sleep(1)

            # 4. Clic en el botón de Login
            # Usamos 'name' ya que es más estable que los IDs dinámicos de FB en el botón
            print("🚀 Iniciando sesión...")
            self._wait_and_click(By.NAME, "login", timeout=30, retries=3)
            
            # Espera adicional después del login para que la página procese
            time.sleep(5)

            # 5. Verificación de Login Exitoso
            # Esperamos ver un elemento típico del feed (ej. barra de navegación o historias)
            # Intentamos múltiples selectores para mayor robustez
            print("🔍 Verificando login exitoso...")
            success = False
            selectors = [
                (By.CSS_SELECTOR, "[aria-label='Facebook']"),
                (By.CSS_SELECTOR, "[aria-label='Home']"),
                (By.CSS_SELECTOR, "[role='navigation']"),
                (By.XPATH, "//a[@href='/']")
            ]
            
            for by, selector in selectors:
                try:
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    success = True
                    break
                except TimeoutException:
                    continue
            
            if success:
                print("✅ Login exitoso. Estás dentro.")
            else:
                print("⚠️ No se pudo verificar el login completamente, pero el proceso continuó.")

            # Mantener el navegador abierto un momento para verificar visualmente
            time.sleep(50)

        except TimeoutException:
            print("❌ Error: Tiempo de espera agotado después de múltiples reintentos.")
            print("💡 Sugerencias:")
            print("   - Verifica tu conexión a internet")
            print("   - Facebook puede estar bloqueando el acceso automatizado")
            print("   - Intenta ejecutar el script nuevamente")
        except StaleElementReferenceException:
            print("⚠️ Error: El elemento cambió en el DOM mientras intentaba interactuar.")
            print("💡 Intenta ejecutar el script nuevamente.")
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
        finally:
            print("👋 Cerrando navegador...")
            self.driver.quit()

if __name__ == "__main__":
    if not os.getenv("FB_EMAIL") or not os.getenv("FB_PASSWORD"):
        print("⚠️ Error: Asegúrate de configurar el archivo .env con tus credenciales.")
    else:
        bot = FacebookBot()
        bot.login()