import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard    from './pages/Dashboard'
import DatasetList  from './pages/DatasetList'
import Upload       from './pages/Upload'
import AlertsPage   from './pages/AlertsPage'
import DatasetDetail from './pages/DatasetDetail'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard"         element={<Dashboard />} />
        <Route path="/datasets"          element={<DatasetList />} />
        <Route path="/dataset/:id"       element={<DatasetDetail />} />
        <Route path="/upload"            element={<Upload />} />
        <Route path="/alerts"            element={<AlertsPage />} />
        <Route path="*"                  element={<Navigate to="/dashboard" replace />} />
      </Route>
    </Routes>
  )
}
