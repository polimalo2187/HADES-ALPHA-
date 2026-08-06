import { useState } from 'react'
import Login from './components/Login'
import Dashboard from './components/Dashboard'
import { useAuthStore } from './context/authStore'

function App() {
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated)
  const [showDashboard, setShowDashboard] = useState(isAuthenticated)

  const handleLoginSuccess = () => {
    setShowDashboard(true)
  }

  return (
    <>
      {!showDashboard ? (
        <Login onLoginSuccess={handleLoginSuccess} />
      ) : (
        <Dashboard />
      )}
    </>
  )
}

export default App
