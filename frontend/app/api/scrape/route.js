import { NextResponse } from 'next/server'

const AIRFLOW_API_URL = process.env.AIRFLOW_API_URL || 'http://airflow-apiserver:8080'
const AIRFLOW_USER = process.env.AIRFLOW_USER || 'airflow'
const AIRFLOW_PASSWORD = process.env.AIRFLOW_PASSWORD || 'airflow'

async function getAirflowToken() {
  try {
    const tokenUrl = `${AIRFLOW_API_URL}/auth/token`
    
    const response = await fetch(tokenUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        username: AIRFLOW_USER,
        password: AIRFLOW_PASSWORD
      })
    })
    
    if (!response.ok) {
      console.error('[TOKEN] Erreur obtention token:', await response.text())
      return null
    }
    
    const data = await response.json()
    const token = data.access_token || data.token
    
    if (!token) {
      console.error('[TOKEN] Aucun token dans la réponse')
      return null
    }
    
    console.log('[TOKEN] Token JWT obtenu avec succès')
    return token
    
  } catch (error) {
    console.error('[TOKEN] Erreur:', error)
    return null
  }
}

export async function POST(request) {
  try {
    const { entity_number } = await request.json()
    
    if (!entity_number) {
      return NextResponse.json(
        { error: 'Numéro d\'entreprise requis' },
        { status: 400 }
      )
    }
    
    // Valider le format du numéro (10 chiffres avec ou sans points)
    const cleanNumber = entity_number.replace(/\./g, '')
    if (!/^0\d{9}$/.test(cleanNumber)) {
      return NextResponse.json(
        { error: 'Format de numéro invalide. Attendu: 0XXX.XXX.XXX' },
        { status: 400 }
      )
    }
    
    console.log('[SCRAPE] Déclenchement du scraping pour:', entity_number)
    
    // Obtenir le token JWT
    const token = await getAirflowToken()
    if (!token) {
      return NextResponse.json(
        { error: 'Impossible d\'obtenir le token d\'authentification' },
        { status: 500 }
      )
    }
    
    // Déclencher le DAG Airflow avec l'API v2
    const dagRunId = `scrape_${cleanNumber}_${Date.now()}`
    const airflowUrl = `${AIRFLOW_API_URL}/api/v2/dags/dag_search_simple/dagRuns`
    
    // Générer la logical_date (date actuelle en ISO format)
    const logicalDate = new Date().toISOString()
    
    console.log('[SCRAPE] URL Airflow:', airflowUrl)
    console.log('[SCRAPE] DAG Run ID:', dagRunId)
    console.log('[SCRAPE] Logical Date:', logicalDate)
    
    const airflowResponse = await fetch(airflowUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      },
      body: JSON.stringify({
        dag_run_id: dagRunId,
        logical_date: logicalDate,
        conf: {
          entity_number: entity_number
        }
      })
    })
    
    const responseText = await airflowResponse.text()
    console.log('[SCRAPE] Réponse Airflow:', responseText)
    
    if (!airflowResponse.ok) {
      console.error('[SCRAPE] Erreur Airflow:', responseText)
      return NextResponse.json(
        { 
          error: 'Erreur lors du déclenchement du scraping',
          details: responseText
        },
        { status: 500 }
      )
    }
    
    const result = JSON.parse(responseText)
    console.log('[SCRAPE] DAG déclenché avec succès:', dagRunId)
    
    return NextResponse.json({
      success: true,
      message: 'Scraping lancé avec succès',
      dag_run_id: dagRunId,
      entity_number: entity_number,
      airflow_response: result
    })
    
  } catch (error) {
    console.error('[SCRAPE] Erreur:', error)
    return NextResponse.json(
      { 
        error: 'Erreur serveur',
        details: error.message
      },
      { status: 500 }
    )
  }
}