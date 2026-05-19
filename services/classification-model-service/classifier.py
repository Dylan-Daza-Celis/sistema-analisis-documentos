import re
import unicodedata
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline

# Muestras de entrenamiento - CON 8 TEMAS COMPLETOS
MUESTRAS_BASE = {
    "Matematicas": [
        "algebra lineal calculo diferencial ecuaciones diferenciales optimizacion",
        "teorema demostracion geometria analitica probabilidad estadistica",
        "metodos numericos analisis funcional series fourier",
        "teoria grafos combinatoria logica matematica",
        "modelado matematico derivadas integrales variables complejas",
        "inferencia estadistica distribuciones muestreo estimacion",
        "topologia espacios metricos continuidad convergencia",
        "probabilidad procesos estocasticos cadenas markov",
        "estadistica bayesiana inferencia modelos jerarquicos",
        "optimizacion convexa programacion lineal no lineal",
        "algebra abstracta grupos anillos cuerpos",
        "teoria numeros criptografia modular congruencias",
        "analisis numerico interpolacion error estabilidad",
        "ecuaciones diferenciales parciales condiciones frontera",
        "geometria diferencial variedades curvatura",
        "calculo variacional funcionales extremos",
        "numeros reales complejos sucesiones series infinitas",
        "limites continuidad derivabilidad integrabilidad",
        "espacios vectoriales matrices determinantes sistemas lineales",
        "polinomios factorizacion raices teorema fundamental",
        "trigonometria identidades funciones trigonometricas",
        "vectores producto escalar cruz angulos geometria",
        "matrices transformaciones lineales autovalores autovectores",
        "sistemas ecuaciones lineales gauss jordan eliminacion",
        "calculo vectorial gradiente divergencia rotacional",
        "funciones multivariables maximos minimos lagrange",
        "series numericas convergencia divergencia radio convergencia",
        "ecuaciones diferenciales ordinarias soluciones particulares",
        "transformada laplace fourier ecuaciones diferenciales",
        "analisis complejo singularidades residuos integrales",
    ],
    "Biologia": [
        "biologia celular genetica adn evolucion ecologia",
        "microbiologia fisiologia organismo especie ecosistema",
        "biotecnologia biodiversidad botanica zoologia",
        "clonacion celulas madre metabolismo fotosintesis",
        "taxonomia filogenia reinos biologicos seleccion natural",
        "genoma proteoma transcriptoma bioinformatica",
        "ecologia poblaciones comunidades biomas",
        "neurobiologia endocrinologia inmunologia",
        "paleontologia fosiles deriva continental",
        "biologia marina oceanografia arrecifes",
        "virologia bactericidas hongos micologia",
        "ciclo krebs glucolisis fosforilacion oxidativa",
        "meiosis mitosis ciclo celular replicacion adn",
        "herencia mendeliana alelos locus fenotipo",
        "etologia comportamiento animal apareamiento",
        "anatomia humana sistemas organos tejidos",
        "fisiologia cardiovascular circulatorio respiratorio",
        "bioquimica proteinas lipidos carbohidratos enzimas",
        "histologia microscopía biologia organelas",
        "microbiologia bacterias arqueas eucariotas",
        "parasitologia enfermedades infecciosas hospedador",
        "inmunologia anticuerpos antígenos respuesta inmune",
        "hormonas regulacion endocrina glándulas secretoras",
        "nutricion metabolismo calorías macronutrientes",
        "oftalmologia vision ocular óptica retina",
        "genética herencia genes cromosomas",
        "evolución darwinismo adaptacion seleccion",
        "filogenia cladistica arboles evolutivos ancestros",
        "ecologia habitat nicho cadena alimentaria",
        "biologia forestal silvicultura ecosistemas",
    ],
    "Computacion": [
        "programacion algoritmos estructuras datos bases datos",
        "inteligencia artificial redes neuronales aprendizaje automatico",
        "sistemas operativos arquitectura computadoras redes",
        "desarrollo web frontend backend fullstack",
        "seguridad informatica criptografia hacking etico",
        "computacion nube microservicios docker kubernetes",
        "lenguajes programacion compiladores interpretes",
        "ingenieria software metodologias agiles scrum",
        "realidad virtual aumentada procesamiento imagenes",
        "blockchain criptomonedas contratos inteligentes",
        "internet cosas dispositivos embebidos sensores",
        "mineria datos big data analitica datos",
        "interaccion hombre maquina usabilidad interfaces",
        "computacion grafica renderizado motores juegos",
        "sistemas distribuidos tolerancia fallos escalabilidad",
        "linux bash shell comandos terminal administrador",
        "python java cpp csharp javascript programming",
        "git github versionamiento control cambios repositorio",
        "html css responsive diseño web interfaces",
        "react angular vue typescript frameworks",
        "nodejs express mongodb rest api json",
        "sql bases datos relacional normalizacion",
        "testing unitario integracion calidad software",
        "cicd devops automatizacion despliegue pipelines",
        "machine learning clasificacion regression clustering",
        "deep learning redes convolucionales lstm",
        "procesamiento natural lenguaje nlp texto",
        "computacion paralela gpu cuda threading",
        "compiladores lexer parser generador codigo",
        "protocolo http tcp ip dns redes comunicacion",
    ],
    "Humanidades": [
        "literatura narrativa poesia ensayo novela drama",
        "linguistica sintaxis morfologia fonetica lengua",
        "filologia dialectos etimologia linguistica comparada",
        "historia cultura sociedad pensamiento humanista",
        "hermeneutica interpretacion textual retorica",
        "filosofia etica epistemologia metafisica ontologia",
        "historia arte estetica patrimonio iconografia",
        "antropologia social etnografia culturas tradiciones",
        "arqueologia patrimonio cultural artefactos civilizacion",
        "sociologia teorias sociales modernidad urbanismo",
        "historiografia fuentes archivos memoria colectiva",
        "literatura comparada teoria literaria critica textual",
        "semiotica signos simbolos significado interpretacion",
        "didactica educacion pedagogia aprendizaje docencia",
        "retorica persuasion argumentacion oratoria discurso",
        "fenomenologia experiencia vivida conciencia existencia",
        "estética arte belleza creatividad expresión artística",
        "linguistica formativa dialectos sociolinguistica variación",
        "narratologia cuento relato estructura trama protagonista",
        "critica cultural sociedades ideologia poder discurso",
        "teatro dramatologia dramaturgia personajes escena",
        "poesia metafora ritmo verso prosa lirica",
        "traduccion interlingual cultural transferencia significado",
        "historia medieval feudalismo caballeria imperio",
        "historia moderna renacimiento reforma ilustracion",
        "arqueologia excavacion estratificacion cultura material",
        "paleografia manuscritos documentos antiguos",
        "numismatica monedas medallones economia",
        "etica valores morales deontologia utilitarismo",
        "ontologia ser existencia realidad naturaleza",
    ],
    "Medicina": [
        "anatomia humana sistemas organos fisiologia clinica",
        "farmacologia terapias medicamentos prescripcion drogas",
        "patologia enfermedades diagnostico etiologia patogenia",
        "cirugia procedimientos quirurgicos tecnicas intervenciones",
        "cardiologia cardiovascular corazon infarto hipertension",
        "neumologia respiratoria pulmones asma neumopatias",
        "gastroenterologia digestivo higado pancreas intestino",
        "neurologia cerebro sistema nervioso epilepsia ictus",
        "psiquiatria trastornos mentales depresion esquizofrenia",
        "oftalmologia vision ocular ojo retina glaucoma",
        "otorrinolaringologia nariz garganta oido audicion",
        "dermatologia piel lesiones infecciones dermatitis",
        "traumatologia ortopedia huesos articulaciones fracturas",
        "oncologia cancer tumores quimioterapia radioterapia",
        "hematologia sangre coagulacion anemias leucemias",
        "radiologia imagenes diagnostico tomografia resonancia",
        "pediatria ninos infancia desarrollo crecimiento",
        "geriatria adultos mayores envejecimiento ancianos",
        "obstetricia ginecologia embarazo parto reproduccion",
        "endocrinologia hormonas metabolismo diabetes",
        "reumatologia artritis enfermedades autoinmunes",
        "urologia riñones vejiga prostata sistema urinario",
        "nefrologia kidney renal glomerulonefritis diálisis",
        "infectologia infecciones bacterianas virales parasitarias",
        "anestesiologia dolor sedacion general regional",
        "cuidados intensivos terapia critica urgencias",
        "epidemiologia salud publica enfermedades poblacion",
        "toxicologia venenos intoxicaciones substancias quimicas",
        "medicina forense legal crimen autopsia",
        "telemedicina diagnostico remoto monitoreo digital",
    ],
    "Historia": [
        "historia antigua mesopotamia egipto grecia roma",
        "edad media feudalismo caballeria imperio cristiano",
        "renacimiento humanismo reforma ilustracion",
        "revolucion francesa americana independencia",
        "historia moderna colonialismo imperio expansion",
        "historia contemporanea siglos diecinueve veinte",
        "historia arte pintura escultura arquitectura",
        "arqueologia excavacion estratificacion cultura",
        "paleografia manuscritos documentos antiguos",
        "numismatica monedas medallones economia",
        "historia militar guerra estrategia batalla",
        "historia politica gobierno estado instituciones",
        "historia social clases grupos marginales",
        "historia cultural tradiciones costumbres folklore",
        "historia ciencia descubrimientos revoluciones",
        "historia tecnologia inventos innovacion progreso",
        "imperio romano república legiones conquista",
        "imperio persa babilonia asiria mesopotamia",
        "antigua china civilizacion dinastias filosofia",
        "edad media castillos castillos feudalismo",
        "renacimiento italiano florencia maquiavelo",
        "reforma protestante lutero calvino cristianismo",
        "ilustracion enciclopedia rousseau voltaire",
        "revolucion industrial mecanizacion produccion",
        "epoca victoriana imperio britanico expansion",
        "guerra mundial conflicto global politica",
        "holocausto genocidio fascismo totalitarismo",
        "post guerra fria cortina hierro division",
        "descolonizacion independencia africa asia",
        "globalizacion modernidad contemporaneo siglo veintiuno",
    ],
    "Psicologia": [
        "psicologia cognitiva percepcion memoria atencion",
        "psicologia desarrollo infancia adolescencia vejez",
        "psicologia social interaccion grupos influencia",
        "psicopatologia trastornos mentales diagnostico",
        "psicologia clinica terapia consejeria intervencion",
        "psicologia educativa aprendizaje motivacion rendimiento",
        "psicologia laboral recursos humanos motivacion estres",
        "psicofisiologia mente cuerpo neuropsicologia cerebro",
        "teoria aprendizaje conductismo cognitivismo humanismo",
        "psicometria pruebas evaluacion diagnostico medicion",
        "psicologia evolutiva instinto comportamiento adaptativo",
        "psicologia ambiental espacios conducta habitat",
        "psicologia comunitaria poblacion intervenciones",
        "psicologia deporte rendimiento atletico motivacion",
        "psicologia consumidor comportamiento compra decision",
        "psicologia forense legal crimen testimonio juzgado",
        "psicoanalisis freud inconsciente defensa complejo",
        "conducta humana estimulo respuesta condicionamiento",
        "cognicion pensamiento razonamiento solucion problemas",
        "emocion sentimiento afecto regulacion emocional",
        "personalidad temperamento caracter rasgos tipologia",
        "inteligencia cognitiva emocional social multiples",
        "motivacion necesidad impulso objetivo meta aspiracion",
        "estres ansiedad depresion trastorno psicologico",
        "autoestima autoimagen autoconcepto valoracion",
        "relaciones interpersonales apego vinculo amor",
        "grupo dinamica social influencia conformidad",
        "liderazgo autoridad poder control influencia",
        "psicologia de masa histeria colectiva comportamiento",
        "investigacion experimental metodo cientifico variables",
    ],
    "Astronomia": [
        "astronomia observacional telescopio espectroscopia",
        "cosmologia universo origen expansion big bang",
        "astrofisica estrellas nucleosintesis fusion nuclear",
        "sistemas planetarios planetas exoplanetas satelites",
        "galaxias estructura dinamica formacion evolucion",
        "relatividad general espaciotiempo gravitacion",
        "mecanica celeste orbitas perturbaciones resonancias",
        "agujeros negros singularidad horizonte eventos",
        "materia oscura energia oscura inflacion cosmologica",
        "radiacion cosmica fondo microondas radiacion",
        "astroquimica elementos moleculas intergalactico",
        "astrobiologia panspermia vida extraterrestre",
        "planetologia formacion composicion dinamica",
        "radiacion ultravioleta rayos gamma infrarrojo",
        "instrumental astronomico camaras espectrografos",
        "supernovas explosiones estelar nucleosintesis",
        "nebulosas gas polvo formacion estrellas",
        "estrellas enanas blancas neutrones gigantes rojas",
        "sistema solar sol planetas asteroides cometas",
        "luna satelites naturales satelites artificiales",
        "marte venus mercurio jupiter saturno",
        "lluvia meteorica meteoros meteoritos impactos",
        "aurora boreal magnetosfera viento solar",
        "eclipses lunares solares alineacion orbital",
        "precesion nutacion movimiento polar tierra",
        "espacio interestelar viajes interplanetarios",
        "fisica cuantica relatividad teoria gravitacion",
        "observatorio astronomico columpio telescopio",
        "espectro electromagnetico luz radiacion",
        "cartografia celeste constelaciones zodiaco",
    ]
}

# Grupos de secciones de articulos cientificos
GRUPOS_SECCIONES = {
    "resumen": ["resumen", "abstract", "summary", "sinopsis"],
    "introduccion": ["introduccion", "introduction", "preambulo", "presentacion"],
    "metodos": ["metodologia", "metodos", "materiales", "desarrollo", "experimental", "procedure"],
    "resultados": ["resultados", "results", "discusion", "conclusiones", "findings"],
    "referencias": ["referencias", "bibliografia", "sources", "references", "literatura citada", "works cited"]
}

# Indicadores de articulos cientificos - EXPANDIDOS
INDICADORES_CIENTIFICOS = [
    "doi", "issn", "isbn", "journal", "revista", "dialnet", "scielo", "redalyc",
    "scopus", "web of science", "pubmed", "arxiv", "researchgate", "citeseer",
    "springer", "elsevier", "proquest", "jstor", "tandfonline", "plos", "crossref",
    "orcid", "abstract", "keywords", "peer review", "submitted", "accepted",
    "conference", "proceeding", "symposium", "impact factor", "article", "research",
    "study", "analysis", "investigation", "experiment", "hypothesis", "methodology",
    "statistical", "data", "conclusion", "theoretical", "empirical", "quantitative",
    "qualitative", "mixed method", "literature review", "systematic review", "meta-analysis",
    "citation", "reference", "author", "institution", "university", "laboratory",
    "published", "editor", "volume", "issue", "pages", "year", "date"
]

def normalizar_texto(texto):
    if not texto:
        return ""
    texto = texto.lower()
    texto = "".join(
        c for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )
    texto = re.sub(r"[^a-z\s]", " ", texto)
    return " ".join(texto.split())

class DocumentClassifier:
    def __init__(self):
        self.modelo = self._entrenar_modelo()

    def _entrenar_modelo(self):
        x_train = []
        y_train = []
        for tema, textos in MUESTRAS_BASE.items():
            for t in textos:
                x_train.append(normalizar_texto(t))
                y_train.append(tema)
        
        pipeline = Pipeline([
            ("tfidf", TfidfVectorizer(
                ngram_range=(1, 2),
                min_df=1,
                max_df=1.0,
                strip_accents='unicode',
                lowercase=True,
                analyzer='word'
            )),
            ("clf", RandomForestClassifier(
                n_estimators=300,
                max_depth=25,
                min_samples_split=2,
                min_samples_leaf=1,
                random_state=42,
                n_jobs=-1,
                class_weight='balanced'
            ))
        ])
        pipeline.fit(x_train, y_train)
        return pipeline

    def evaluar_articulo_cientifico(self, text):
        puntaje = 0
        secciones_encontradas = []
        
        # Convertir texto a minúsculas para búsqueda
        text_min = text.lower()

        # 1. Buscar secciones características de artículos científicos
        for grupo, sinonimos in GRUPOS_SECCIONES.items():
            for sinonimo in sinonimos:
                if re.search(rf"\b{sinonimo}\b", text_min):
                    puntaje += 1
                    secciones_encontradas.append(grupo)
                    break

        # 2. Buscar indicadores científicos
        for indicador in INDICADORES_CIENTIFICOS:
            if indicador in text_min:
                puntaje += 0.5

        # Umbral: 1.5 para detección flexible
        es_cientifico = puntaje >= 1.5
        
        return {
            "es_cientifico": es_cientifico,
            "puntaje": puntaje,
            "secciones": secciones_encontradas
        }

    def classify(self, text):
        if not text or len(text.strip()) < 50:
            return None

        evaluacion = self.evaluar_articulo_cientifico(text)
        
        if not evaluacion["es_cientifico"]:
            return {
                "tema": None,
                "confianza": 0.0,
                "metodo": "articulo_no_cientifico",
                "es_cientifico": False,
                "puntaje": evaluacion["puntaje"],
                "secciones": evaluacion["secciones"],
                "razon": "No se detectaron suficientes indicadores de caracter cientifico"
            }

        text_normalized = normalizar_texto(text)
        probabilidades = self.modelo.predict_proba([text_normalized])[0]
        mejor_indice = max(range(len(probabilidades)), key=lambda i: probabilidades[i])
        tema = self.modelo.classes_[mejor_indice]
        confianza = float(probabilidades[mejor_indice])

        confianza_por_tema = {
            self.modelo.classes_[i]: float(probabilidades[i])
            for i in range(len(probabilidades))
        }

        return {
            "tema": tema,
            "confianza": confianza,
            "metodo": "random_forest",
            "es_cientifico": True,
            "puntaje": evaluacion["puntaje"],
            "secciones": evaluacion["secciones"],
            "confianza_por_tema": confianza_por_tema
        }
