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
    return data.access_token || data.token
    
  } catch (error) {
    console.error('[TOKEN] Erreur:', error)
    return null
  }
}

export async function GET(request, { params }) {
  try {
    const { dag_run_id } = params
    
    console.log('[SCRAPE STATUS] Vérification du statut:', dag_run_id)
    
    // Obtenir le token JWT
    const token = await getAirflowToken()
    if (!token) {
      return NextResponse.json(
        { error: 'Impossible d\'obtenir le token d\'authentification' },
        { status: 500 }
      )
    }
    
    // Utiliser l'API v2
    const airflowUrl = `${AIRFLOW_API_URL}/api/v2/dags/dag_search_simple/dagRuns/${dag_run_id}`
    
    const airflowResponse = await fetch(airflowUrl, {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    })
    
    if (!airflowResponse.ok) {
      const errorText = await airflowResponse.text()
      console.error('[SCRAPE STATUS] Erreur:', errorText)
      return NextResponse.json(
        { error: 'Erreur lors de la récupération du statut', details: errorText },
        { status: 500 }
      )
    }
    
    const data = await airflowResponse.json()
    
    // Mapper les états Airflow
    const state = data.state // 'running', 'success', 'failed', 'queued'
    const isFinished = ['success', 'failed'].includes(state)
    
    return NextResponse.json({
      state: state,
      is_finished: isFinished,
      start_date: data.start_date,
      end_date: data.end_date,
      execution_date: data.execution_date
    })
    
  } catch (error) {
    console.error('[SCRAPE STATUS] Erreur:', error)
    return NextResponse.json(
      { error: 'Erreur serveur', details: error.message },
      { status: 500 }
    )
  }
}