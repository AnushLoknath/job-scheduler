import React, { useState, useEffect, useRef } from 'react';

const API_BASE = 'http://localhost:3001/api';

export default function App() {
  const [token, setToken] = useState(localStorage.getItem('token') || '');
  const [user, setUser] = useState(null);
  const [projects, setProjects] = useState([]);
  const [selectedProjectId, setSelectedProjectId] = useState('');
  const [activeTab, setActiveTab] = useState('dashboard');
  
  // Auth Screen State
  const [isRegister, setIsRegister] = useState(false);
  const [usernameInput, setUsernameInput] = useState('');
  const [emailInput, setEmailInput] = useState('');
  const [passwordInput, setPasswordInput] = useState('');
  const [authError, setAuthError] = useState('');

  // Dashboard Data State
  const [queues, setQueues] = useState([]);
  const [metrics, setMetrics] = useState({
    summary: { queued: 0, scheduled: 0, claimed: 0, running: 0, completed: 0, failed: 0, cancelled: 0, dlq: 0 },
    workers: [],
    throughput: []
  });
  
  // Job Explorer State
  const [jobs, setJobs] = useState([]);
  const [explorerFilterQueue, setExplorerFilterQueue] = useState('');
  const [explorerFilterStatus, setExplorerFilterStatus] = useState('');
  const [explorerSearch, setExplorerSearch] = useState('');
  const [explorerPage, setExplorerPage] = useState(1);
  const [explorerTotalPages, setExplorerTotalPages] = useState(1);
  const [inspectedJobId, setInspectedJobId] = useState(null);
  const [inspectedJobData, setInspectedJobData] = useState(null);

  // Retry policies & AI failure diagnostic state
  const [retryPolicies, setRetryPolicies] = useState([]);
  const [creatorRetryPolicyId, setCreatorRetryPolicyId] = useState('');
  const [aiAnalysisSummary, setAiAnalysisSummary] = useState(null);
  const [loadingAiSummary, setLoadingAiSummary] = useState(false);

  // Job Creator State
  const [creatorName, setCreatorName] = useState('Process Payment Task');
  const [creatorQueueId, setCreatorQueueId] = useState('');
  const [creatorPayload, setCreatorPayload] = useState('{\n  "duration_ms": 2000,\n  "fail_rate": 0.2,\n  "error_message": "Payment processor timed out"\n}');
  const [creatorType, setCreatorType] = useState('immediate'); // immediate, delayed, cron, batch, dependency
  const [creatorDelaySecs, setCreatorDelaySecs] = useState('5');
  const [creatorCron, setCreatorCron] = useState('*/2 * * * *');
  const [creatorBatchCount, setCreatorBatchCount] = useState('5');
  const [creatorParentJobId, setCreatorParentJobId] = useState('');

  const [creatorStatusMessage, setCreatorStatusMessage] = useState('');
  const [creatorStatusType, setCreatorStatusType] = useState(''); // success, error

  // SSE Reference
  const sseSourceRef = useRef(null);

  // 1. Initial Load & Fetch Profile
  useEffect(() => {
    if (token) {
      fetchProfile();
    }
  }, [token]);

  // 2. Fetch profile, projects, and initial workspace
  const fetchProfile = async () => {
    try {
      const res = await fetch(`${API_BASE}/auth/me`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const data = await res.json();
      if (res.ok) {
        setUser(data.user);
        setProjects(data.projects);
        if (data.projects.length > 0) {
          setSelectedProjectId(data.projects[0].id);
        }
      } else {
        logout();
      }
    } catch (err) {
      console.error('Fetch profile failed:', err);
      logout();
    }
  };

  // 3. Connect to SSE Live Feed
  useEffect(() => {
    if (!token || !selectedProjectId) return;

    // Fetch initial datasets
    fetchQueues();
    fetchMetrics();
    fetchRetryPolicies();
    if (activeTab === 'explorer') {
      fetchJobs();
    }

    // Connect Server-Sent Events
    const sse = new EventSource(`${API_BASE}/live`);
    sseSourceRef.current = sse;

    sse.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === 'tick' || payload.type === 'worker_update') {
          // Trigger data refresh on heartbeat ticks or membership updates
          fetchQueues();
          fetchMetrics();
        }
      } catch (err) {
        console.error('SSE message parsing failed:', err);
      }
    };

    sse.onerror = (err) => {
      console.warn('SSE connection disconnected. Retrying...');
    };

    return () => {
      sse.close();
    };
  }, [token, selectedProjectId, activeTab, explorerPage, explorerFilterQueue, explorerFilterStatus]);

  // Refresh helper on demand
  useEffect(() => {
    if (token && selectedProjectId && activeTab === 'explorer') {
      fetchJobs();
    }
  }, [explorerSearch]);

  // Polling logs for inspected job details when modal is open
  useEffect(() => {
    let interval;
    if (inspectedJobId) {
      fetchInspectedJob();
      fetchAiSummary(inspectedJobId);
      interval = setInterval(fetchInspectedJob, 2000); // refresh inspector details/logs every 2s
    } else {
      setAiAnalysisSummary(null);
    }
    return () => clearInterval(interval);
  }, [inspectedJobId]);

  const fetchRetryPolicies = async () => {
    if (!selectedProjectId) return;
    try {
      const res = await fetch(`${API_BASE}/retry-policies?project_id=${selectedProjectId}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const data = await res.json();
      if (res.ok) {
        setRetryPolicies(data);
      }
    } catch (err) {
      console.error('Fetch retry policies error:', err);
    }
  };

  const fetchAiSummary = async (jobId) => {
    setLoadingAiSummary(true);
    setAiAnalysisSummary(null);
    try {
      const res = await fetch(`${API_BASE}/jobs/${jobId}/ai-summary`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const data = await res.json();
      if (res.ok) {
        setAiAnalysisSummary(data);
      }
    } catch (err) {
      console.error('Fetch AI summary error:', err);
    } finally {
      setLoadingAiSummary(false);
    }
  };

  // Default queue pre-selection for creator tab
  useEffect(() => {
    if (queues.length > 0 && !creatorQueueId) {
      const defaultQ = queues.find(q => q.name === 'default') || queues[0];
      setCreatorQueueId(defaultQ.id);
    }
  }, [queues]);

  const fetchQueues = async () => {
    if (!selectedProjectId) return;
    try {
      const res = await fetch(`${API_BASE}/queues?project_id=${selectedProjectId}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const data = await res.json();
      if (res.ok) {
        setQueues(data);
      }
    } catch (err) {
      console.error('Fetch queues error:', err);
    }
  };

  const fetchMetrics = async () => {
    if (!selectedProjectId) return;
    try {
      const res = await fetch(`${API_BASE}/metrics?project_id=${selectedProjectId}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const data = await res.json();
      if (res.ok) {
        setMetrics(data);
      }
    } catch (err) {
      console.error('Fetch metrics error:', err);
    }
  };

  const fetchJobs = async () => {
    if (!selectedProjectId) return;
    try {
      let url = `${API_BASE}/jobs?limit=10&page=${explorerPage}`;
      if (explorerFilterQueue) url += `&queue_id=${explorerFilterQueue}`;
      if (explorerFilterStatus) url += `&status=${explorerFilterStatus}`;
      if (explorerSearch) url += `&search=${encodeURIComponent(explorerSearch)}`;

      const res = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const data = await res.json();
      if (res.ok) {
        setJobs(data.jobs);
        setExplorerTotalPages(data.pagination.pages);
      }
    } catch (err) {
      console.error('Fetch jobs error:', err);
    }
  };

  const fetchInspectedJob = async () => {
    if (!inspectedJobId) return;
    try {
      const res = await fetch(`${API_BASE}/jobs/${inspectedJobId}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const data = await res.json();
      if (res.ok) {
        setInspectedJobData(data);
      }
    } catch (err) {
      console.error('Fetch inspected job error:', err);
    }
  };

  // Auth Submit Handlers
  const handleAuthSubmit = async (e) => {
    e.preventDefault();
    setAuthError('');
    const path = isRegister ? 'register' : 'login';
    const payload = isRegister 
      ? { username: usernameInput, email: emailInput, password: passwordInput }
      : { username: usernameInput, password: passwordInput };

    try {
      const res = await fetch(`${API_BASE}/auth/${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (res.ok) {
        localStorage.setItem('token', data.token);
        setToken(data.token);
        setUser(data.user);
        setUsernameInput('');
        setEmailInput('');
        setPasswordInput('');
      } else {
        setAuthError(data.error || 'Authentication failed');
      }
    } catch (err) {
      setAuthError('Cannot connect to authorization server');
    }
  };

  const logout = () => {
    localStorage.removeItem('token');
    setToken('');
    setUser(null);
    setProjects([]);
    setSelectedProjectId('');
    setQueues([]);
  };

  // Queue Configuration Patch Action
  const updateQueueConfig = async (queueId, field, value) => {
    try {
      const res = await fetch(`${API_BASE}/queues/${queueId}`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`
        },
        body: JSON.stringify({ [field]: value })
      });
      if (res.ok) {
        fetchQueues();
      }
    } catch (err) {
      console.error('Failed to patch queue configuration:', err);
    }
  };

  // Submit Jobs Creation
  const handleCreateJob = async (e) => {
    e.preventDefault();
    setCreatorStatusMessage('');
    setCreatorStatusType('');

    // Payload verification
    let parsedPayload = {};
    try {
      parsedPayload = JSON.parse(creatorPayload);
    } catch (err) {
      setCreatorStatusMessage('JSON syntax error in payload field');
      setCreatorStatusType('error');
      return;
    }

    try {
      if (creatorType === 'batch') {
        const jobsArray = [];
        const count = Number(creatorBatchCount) || 5;
        for (let i = 1; i <= count; i++) {
          jobsArray.push({
            name: `${creatorName} #${i}`,
            payload: { ...parsedPayload, index: i },
            max_retries: 3
          });
        }

        const res = await fetch(`${API_BASE}/jobs/batch`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`
          },
          body: JSON.stringify({
            project_id: selectedProjectId,
            queue_id: creatorQueueId,
            name: `${creatorName} Batch`,
            jobs: jobsArray
          })
        });
        const data = await res.json();
        if (res.ok) {
          setCreatorStatusMessage(`Successfully launched batch containing ${count} jobs! (Batch ID: ${data.batch_id})`);
          setCreatorStatusType('success');
        } else {
          setCreatorStatusMessage(data.error || 'Failed to dispatch batch jobs');
          setCreatorStatusType('error');
        }
      } else {
        // Individual, delayed, cron, or dependency jobs
        const payloadBody = {
          queue_id: creatorQueueId,
          name: creatorName,
          payload: parsedPayload,
          retry_policy_id: creatorRetryPolicyId || undefined
        };

        if (creatorType === 'delayed') {
          payloadBody.delay_ms = (Number(creatorDelaySecs) || 5) * 1000;
        } else if (creatorType === 'cron') {
          payloadBody.cron_expression = creatorCron;
        } else if (creatorType === 'dependency' && creatorParentJobId) {
          payloadBody.dependencies = [creatorParentJobId];
        }

        const res = await fetch(`${API_BASE}/jobs`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`
          },
          body: JSON.stringify(payloadBody)
        });
        const data = await res.json();
        if (res.ok) {
          setCreatorStatusMessage(`Job dispatched successfully! (Job ID: ${data.id})`);
          setCreatorStatusType('success');
        } else {
          setCreatorStatusMessage(data.error || 'Failed to create job');
          setCreatorStatusType('error');
        }
      }
    } catch (err) {
      setCreatorStatusMessage('Network connection error when creating job');
      setCreatorStatusType('error');
    }
  };

  // Job Actions inside Drawer
  const handleRetryJob = async (jobId) => {
    try {
      const res = await fetch(`${API_BASE}/jobs/${jobId}/retry`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}` }
      });
      if (res.ok) {
        fetchInspectedJob();
        fetchJobs();
        fetchMetrics();
      }
    } catch (err) {
      console.error('Failed to trigger retry:', err);
    }
  };

  const handleCancelJob = async (jobId) => {
    try {
      const res = await fetch(`${API_BASE}/jobs/${jobId}/cancel`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}` }
      });
      if (res.ok) {
        fetchInspectedJob();
        fetchJobs();
        fetchMetrics();
      }
    } catch (err) {
      console.error('Failed to cancel job:', err);
    }
  };

  // Auth Guard Screen Render
  if (!token) {
    return (
      <div className="auth-container">
        <div className="glass-panel auth-card">
          <div className="auth-header">
            <div className="auth-logo">CODITY<span>.IO</span></div>
            <h2>{isRegister ? 'Create Account' : 'Sign In'}</h2>
            <p style={{ color: 'var(--text-secondary)', marginTop: '8px', fontSize: '14px' }}>
              Distributed Scheduler Coordinator Panel
            </p>
          </div>
          {authError && (
            <div style={{ background: 'rgba(239,68,68,0.1)', color: 'var(--color-danger)', padding: '12px', borderRadius: '8px', fontSize: '13px', marginBottom: '20px', border: '1px solid rgba(239,68,68,0.2)' }}>
              {authError}
            </div>
          )}
          <form onSubmit={handleAuthSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            {isRegister && (
              <div className="form-group">
                <label>Email Address</label>
                <input 
                  type="email" 
                  className="glass-input" 
                  placeholder="name@company.com"
                  value={emailInput}
                  onChange={(e) => setEmailInput(e.target.value)}
                  required 
                />
              </div>
            )}
            <div className="form-group">
              <label>Username</label>
              <input 
                type="text" 
                className="glass-input" 
                placeholder="developer"
                value={usernameInput}
                onChange={(e) => setUsernameInput(e.target.value)}
                required 
              />
            </div>
            <div className="form-group">
              <label>Password</label>
              <input 
                type="password" 
                className="glass-input" 
                placeholder="••••••••"
                value={passwordInput}
                onChange={(e) => setPasswordInput(e.target.value)}
                required 
              />
            </div>
            <button type="submit" className="glass-button primary" style={{ marginTop: '10px' }}>
              {isRegister ? 'Register & Setup Workspace' : 'Enter Dashboard'}
            </button>
          </form>

          <div style={{ textAlign: 'center', marginTop: '24px', fontSize: '13px', color: 'var(--text-secondary)' }}>
            {isRegister ? 'Already have an account?' : 'New to Codity?'}
            <button 
              onClick={() => { setIsRegister(!isRegister); setAuthError(''); }}
              style={{ background: 'none', border: 'none', color: 'var(--color-primary)', fontWeight: '600', marginLeft: '6px', cursor: 'pointer' }}
            >
              {isRegister ? 'Sign In' : 'Register Account'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Draw throughput graph coordinates
  const renderThroughputGraph = () => {
    const data = metrics.throughput || [];
    if (data.length === 0) {
      return (
        <div style={{ height: '200px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)', fontSize: '13px' }}>
          Waiting for workers to process jobs (no throughput history)...
        </div>
      );
    }

    const width = 600;
    const height = 180;
    const paddingLeft = 40;
    const paddingRight = 20;
    const paddingTop = 20;
    const paddingBottom = 30;

    const chartWidth = width - paddingLeft - paddingRight;
    const chartHeight = height - paddingTop - paddingBottom;

    const maxCount = Math.max(...data.map(d => d.count), 5);

    // Map data to x, y coordinates
    const coords = data.map((d, index) => {
      const x = paddingLeft + (index / (data.length - 1 || 1)) * chartWidth;
      const y = paddingTop + chartHeight - (d.count / maxCount) * chartHeight;
      return { x, y, label: d.time, val: d.count };
    });

    // Generate SVG path string
    let pathD = `M ${coords[0].x} ${coords[0].y}`;
    for (let i = 1; i < coords.length; i++) {
      pathD += ` L ${coords[i].x} ${coords[i].y}`;
    }

    // Gradient area path string
    const areaD = `${pathD} L ${coords[coords.length - 1].x} ${height - paddingBottom} L ${coords[0].x} ${height - paddingBottom} Z`;

    return (
      <svg className="graph-svg" viewBox={`0 0 ${width} ${height}`}>
        <defs>
          <linearGradient id="chartGlow" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-primary)" stopOpacity="0.3" />
            <stop offset="100%" stopColor="var(--color-primary)" stopOpacity="0.0" />
          </linearGradient>
        </defs>

        {/* Grid lines */}
        {[0, 0.25, 0.5, 0.75, 1].map((ratio, i) => {
          const y = paddingTop + ratio * chartHeight;
          const val = Math.round(maxCount * (1 - ratio));
          return (
            <g key={i}>
              <line x1={paddingLeft} y1={y} x2={width - paddingRight} y2={y} stroke="rgba(255,255,255,0.04)" strokeDasharray="3,3" />
              <text x={paddingLeft - 10} y={y + 4} fill="var(--text-muted)" fontSize="10" textAnchor="end">{val}</text>
            </g>
          );
        })}

        {/* Area fill */}
        <path d={areaD} fill="url(#chartGlow)" />

        {/* Flow Line */}
        <path d={pathD} fill="none" stroke="var(--color-primary)" strokeWidth="2.5" strokeLinecap="round" />

        {/* Nodes */}
        {coords.map((c, i) => (
          <g key={i}>
            <circle cx={c.x} cy={c.y} r="4" fill="#000" stroke="var(--color-primary)" strokeWidth="2" />
            <text x={c.x} y={height - 10} fill="var(--text-muted)" fontSize="9" textAnchor="middle">{c.label}</text>
          </g>
        ))}
      </svg>
    );
  };

  // Count active vs max concurrency across queues
  const activeJobsRunning = queues.reduce((sum, q) => sum + (q.stats?.running || 0), 0);
  const maxConcurrencyAllocated = queues.reduce((sum, q) => sum + q.concurrency_limit, 0);

  return (
    <div className="app-layout">
      {/* Sidebar Navigation */}
      <aside className="sidebar">
        <div>
          <div className="sidebar-brand">CODITY<span>.IO</span></div>
          <p style={{ fontSize: '10px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginTop: '4px' }}>
            Job Scheduler Dashboard
          </p>
        </div>

        <ul className="nav-links">
          <li className="nav-link-item">
            <span className={`nav-link ${activeTab === 'dashboard' ? 'active' : ''}`} onClick={() => setActiveTab('dashboard')}>
              Telemetry Dashboard
            </span>
          </li>
          <li className="nav-link-item">
            <span className={`nav-link ${activeTab === 'queues' ? 'active' : ''}`} onClick={() => setActiveTab('queues')}>
              Queue Settings
            </span>
          </li>
          <li className="nav-link-item">
            <span className={`nav-link ${activeTab === 'creator' ? 'active' : ''}`} onClick={() => setActiveTab('creator')}>
              Dispatch Job
            </span>
          </li>
          <li className="nav-link-item">
            <span className={`nav-link ${activeTab === 'explorer' ? 'active' : ''}`} onClick={() => setActiveTab('explorer')}>
              Job Explorer
            </span>
          </li>
        </ul>

        {user && (
          <div className="sidebar-user">
            <div className="user-info">
              <span className="username">@{user.username}</span>
              <span>{user.email}</span>
            </div>
            <button className="glass-button" style={{ width: '100%', fontSize: '12px', padding: '8px 12px' }} onClick={logout}>
              Sign Out
            </button>
          </div>
        )}
      </aside>

      {/* Main Panel Content */}
      <main className="main-panel">
        <header className="main-header">
          <div className="project-selector">
            <span style={{ fontSize: '14px', color: 'var(--text-secondary)' }}>Active Project:</span>
            {projects.length > 0 ? (
              <select 
                className="project-select"
                value={selectedProjectId}
                onChange={(e) => setSelectedProjectId(e.target.value)}
              >
                {projects.map(p => (
                  <option key={p.id} value={p.id}>{p.name}</option>
                ))}
              </select>
            ) : (
              <span style={{ fontSize: '14px', color: 'var(--text-primary)' }}>Loading Projects...</span>
            )}
          </div>
          <div style={{ fontSize: '12px', display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-muted)' }}>
            <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: 'var(--color-success)', display: 'inline-block', boxShadow: '0 0 8px var(--color-success-glow)' }} />
            SSE Sync Connected
          </div>
        </header>

        {/* Global Statistics Cards */}
        <section className="stats-grid">
          <div className="glass-panel stat-card">
            <div className="stat-card-title">Active Workers</div>
            <div className="stat-card-value">
              {metrics.workers.filter(w => w.status === 'active').length}
              <span style={{ fontSize: '16px', color: 'var(--text-muted)', fontWeight: '400' }}>
                / {metrics.workers.length} registered
              </span>
            </div>
            <div className="stat-card-subtitle">Heartbeat coordinator online</div>
          </div>
          <div className="glass-panel stat-card">
            <div className="stat-card-title">Allocated Load</div>
            <div className="stat-card-value">
              {activeJobsRunning}
              <span style={{ fontSize: '16px', color: 'var(--text-muted)', fontWeight: '400' }}>
                / {maxConcurrencyAllocated} slots
              </span>
            </div>
            <div className="stat-card-subtitle">Active queue execution limit</div>
          </div>
          <div className="glass-panel stat-card">
            <div className="stat-card-title">Queue Depth</div>
            <div className="stat-card-value">
              {metrics.summary.queued + metrics.summary.scheduled}
            </div>
            <div className="stat-card-subtitle">
              {metrics.summary.queued} immediate • {metrics.summary.scheduled} scheduled
            </div>
          </div>
          <div className="glass-panel stat-card" style={{ borderLeft: metrics.summary.dlq > 0 ? '2px solid var(--color-danger)' : '1px solid var(--border-color)' }}>
            <div className="stat-card-title">Dead Letter Queue</div>
            <div className="stat-card-value" style={{ color: metrics.summary.dlq > 0 ? 'var(--color-danger)' : 'var(--text-primary)' }}>
              {metrics.summary.dlq}
            </div>
            <div className="stat-card-subtitle" style={{ color: metrics.summary.dlq > 0 ? 'var(--color-danger)' : 'var(--text-muted)' }}>
              {metrics.summary.dlq > 0 ? 'Requires developer intervention' : 'Healthy (0 toxic jobs)'}
            </div>
          </div>
        </section>

        {/* Tabs Body */}
        {activeTab === 'dashboard' && (
          <div className="dashboard-columns">
            <div>
              {/* Chart */}
              <div className="glass-panel graph-container">
                <div className="graph-header">
                  <h3>Throughput Telemetry</h3>
                  <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
                    Jobs completed / minute (15m window)
                  </span>
                </div>
                {renderThroughputGraph()}
              </div>

              {/* Quick queue status bar */}
              <div className="glass-panel" style={{ padding: '24px' }}>
                <h3 style={{ marginBottom: '16px' }}>Active System Queues</h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                  {queues.map(q => {
                    const running = q.stats?.running || 0;
                    const pct = Math.min((running / q.concurrency_limit) * 100, 100);
                    return (
                      <div key={q.id} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px', background: 'rgba(255,255,255,0.01)', border: '1px solid var(--border-color)', borderRadius: '6px' }}>
                        <div>
                          <strong style={{ display: 'block', fontSize: '14px' }}>{q.name}</strong>
                          <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                            Priority: {q.priority} • Concurrency: {q.concurrency_limit} max
                          </span>
                        </div>
                        <div style={{ width: '150px', textAlign: 'right' }}>
                          <span style={{ fontSize: '12px', fontWeight: '600', display: 'block', marginBottom: '4px' }}>
                            {running} / {q.concurrency_limit} running
                          </span>
                          <div className="capacity-bar" style={{ width: '100%' }}>
                            <div className="capacity-fill" style={{ width: `${pct}%`, background: q.is_paused ? 'var(--color-neutral)' : pct > 80 ? 'var(--color-warning)' : 'var(--color-success)' }} />
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>

            {/* Right Column: Workers listing */}
            <div className="glass-panel workers-card">
              <h3>Processing Nodes</h3>
              <div className="worker-list">
                {metrics.workers.length === 0 ? (
                  <div style={{ color: 'var(--text-muted)', fontSize: '13px', padding: '10px 0' }}>
                    No worker nodes registered. Start a worker process in the terminal.
                  </div>
                ) : (
                  metrics.workers.map(w => {
                    const isStale = Date.now() - w.last_heartbeat > 15000;
                    const status = isStale ? 'offline' : w.status;
                    return (
                      <div key={w.id} className="worker-item">
                        <div className="worker-header">
                          <span className="worker-name">{w.hostname} ({w.id})</span>
                          <span className={`status-badge ${status === 'active' ? 'completed' : 'failed'}`}>
                            {status === 'active' ? 'Online' : 'Offline'}
                          </span>
                        </div>
                        <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                          Heartbeat: {new Date(w.last_heartbeat).toLocaleTimeString()}
                        </div>
                        <div className="worker-meta">
                          <span>Concurrency: {w.concurrency_limit} thread limit</span>
                          <span>Started: {new Date(w.started_at).toLocaleTimeString()}</span>
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            </div>
          </div>
        )}

        {activeTab === 'queues' && (
          <div className="queues-grid">
            {queues.map(q => {
              const activeCount = q.stats?.claimed + q.stats?.running || 0;
              const barPercentage = Math.min((activeCount / q.concurrency_limit) * 100, 100);

              return (
                <div key={q.id} className="glass-panel queue-card">
                  <div className="queue-card-header">
                    <span className="queue-title">{q.name}</span>
                    <span className={`status-badge ${q.is_paused ? 'failed' : 'completed'}`}>
                      {q.is_paused ? 'Paused' : 'Active'}
                    </span>
                  </div>

                  <div className="queue-config-row">
                    <span>Priority (Higher runs first)</span>
                    <div className="queue-config-control">
                      <button 
                        className="glass-button" 
                        style={{ padding: '2px 8px' }}
                        onClick={() => updateQueueConfig(q.id, 'priority', Math.max(1, q.priority - 5))}
                      >-</button>
                      <strong style={{ minWidth: '24px', textAlign: 'center' }}>{q.priority}</strong>
                      <button 
                        className="glass-button" 
                        style={{ padding: '2px 8px' }}
                        onClick={() => updateQueueConfig(q.id, 'priority', q.priority + 5)}
                      >+</button>
                    </div>
                  </div>

                  <div className="queue-config-row">
                    <span>Concurrency Limit</span>
                    <div className="queue-config-control">
                      <button 
                        className="glass-button" 
                        style={{ padding: '2px 8px' }}
                        onClick={() => updateQueueConfig(q.id, 'concurrency_limit', Math.max(1, q.concurrency_limit - 1))}
                      >-</button>
                      <strong style={{ minWidth: '24px', textAlign: 'center' }}>{q.concurrency_limit}</strong>
                      <button 
                        className="glass-button" 
                        style={{ padding: '2px 8px' }}
                        onClick={() => updateQueueConfig(q.id, 'concurrency_limit', q.concurrency_limit + 1)}
                      >+</button>
                    </div>
                  </div>

                  <div className="queue-config-row" style={{ marginTop: '16px' }}>
                    <span>Active Concurrency Load ({activeCount} / {q.concurrency_limit})</span>
                  </div>
                  <div className="capacity-bar" style={{ marginBottom: '16px' }}>
                    <div className="capacity-fill" style={{ width: `${barPercentage}%`, background: q.is_paused ? 'var(--color-neutral)' : barPercentage > 80 ? 'var(--color-warning)' : 'var(--color-success)' }} />
                  </div>

                  <div className="queue-config-row" style={{ marginTop: '16px' }}>
                    <span>Pause / Resume Execution</span>
                    <label className="toggle-switch">
                      <input 
                        type="checkbox" 
                        checked={!q.is_paused}
                        onChange={(e) => updateQueueConfig(q.id, 'is_paused', !e.target.checked)}
                      />
                      <span className="slider"></span>
                    </label>
                  </div>

                  <div className="queue-config-row" style={{ marginTop: '16px' }}>
                    <span>Retry Policy Configuration</span>
                    <select
                      className="project-select"
                      style={{ padding: '6px 12px', background: 'rgba(255,255,255,0.02)', color: 'var(--text-primary)', border: '1px solid var(--border-color)', borderRadius: '6px' }}
                      value={q.retry_policy_id || ''}
                      onChange={(e) => updateQueueConfig(q.id, 'retry_policy_id', e.target.value || null)}
                    >
                      <option value="">Inherit System Defaults</option>
                      {retryPolicies.map(rp => (
                        <option key={rp.id} value={rp.id}>{rp.name}</option>
                      ))}
                    </select>
                  </div>

                  <div className="queue-stats-mini">
                    <div className="stat-box">
                      <div className="stat-box-num">{q.stats?.queued}</div>
                      <div className="stat-box-label">Queued</div>
                    </div>
                    <div className="stat-box">
                      <div className="stat-box-num">{q.stats?.running}</div>
                      <div className="stat-box-label">Running</div>
                    </div>
                    <div className="stat-box">
                      <div className="stat-box-num">{q.stats?.completed}</div>
                      <div className="stat-box-label">Success</div>
                    </div>
                    <div className="stat-box" style={{ color: q.stats?.dlq > 0 ? 'var(--color-danger)' : 'inherit' }}>
                      <div className="stat-box-num">{q.stats?.dlq}</div>
                      <div className="stat-box-label">DLQ</div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {activeTab === 'creator' && (
          <div className="glass-panel" style={{ maxWidth: '720px', padding: '40px' }}>
            <h2 style={{ marginBottom: '10px' }}>Dispatch Asynchronous Job</h2>
            <p style={{ color: 'var(--text-secondary)', fontSize: '14px', marginBottom: '24px' }}>
              Enqueues immediate, delayed, recurring, batched, or dependent background jobs.
            </p>

            {creatorStatusMessage && (
              <div style={{
                background: creatorStatusType === 'success' ? 'rgba(16,185,129,0.1)' : 'rgba(239,68,68,0.1)',
                color: creatorStatusType === 'success' ? 'var(--color-success)' : 'var(--color-danger)',
                border: creatorStatusType === 'success' ? '1px solid rgba(16,185,129,0.2)' : '1px solid rgba(239,68,68,0.2)',
                padding: '16px',
                borderRadius: '8px',
                fontSize: '14px',
                marginBottom: '24px'
              }}>
                {creatorStatusMessage}
              </div>
            )}

            <form onSubmit={handleCreateJob}>
              <div className="form-grid">
                <div className="form-group">
                  <label>Target Queue</label>
                  <select 
                    className="glass-input" 
                    value={creatorQueueId}
                    onChange={(e) => setCreatorQueueId(e.target.value)}
                    required
                  >
                    {queues.map(q => (
                      <option key={q.id} value={q.id}>{q.name} (Priority {q.priority})</option>
                    ))}
                  </select>
                </div>
                <div className="form-group">
                  <label>Job Name / Action Identifier</label>
                  <input 
                    type="text" 
                    className="glass-input" 
                    value={creatorName}
                    onChange={(e) => setCreatorName(e.target.value)}
                    required 
                  />
                </div>

                <div className="form-group">
                  <label>Trigger Strategy</label>
                  <select 
                    className="glass-input" 
                    value={creatorType} 
                    onChange={(e) => setCreatorType(e.target.value)}
                  >
                    <option value="immediate">Immediate Queueing</option>
                    <option value="delayed">Delayed / Scheduled</option>
                    <option value="cron">Recurring Cron Pattern</option>
                    <option value="batch">Batch Dispatch (Fan-out)</option>
                    <option value="dependency">Workflow Dependency (DAG)</option>
                  </select>
                </div>

                <div className="form-group">
                  <label>Retry Policy Override</label>
                  <select 
                    className="glass-input" 
                    value={creatorRetryPolicyId}
                    onChange={(e) => setCreatorRetryPolicyId(e.target.value)}
                  >
                    <option value="">Inherit Queue Default</option>
                    {retryPolicies.map(rp => (
                      <option key={rp.id} value={rp.id}>{rp.name} ({rp.strategy})</option>
                    ))}
                  </select>
                </div>

                {creatorType === 'delayed' && (
                  <div className="form-group">
                    <label>Delay Before Execution (seconds)</label>
                    <input 
                      type="number" 
                      className="glass-input" 
                      min="1"
                      value={creatorDelaySecs}
                      onChange={(e) => setCreatorDelaySecs(e.target.value)}
                      required 
                    />
                  </div>
                )}

                {creatorType === 'cron' && (
                  <div className="form-group">
                    <label>Cron expression</label>
                    <input 
                      type="text" 
                      className="glass-input" 
                      placeholder="*/2 * * * *"
                      value={creatorCron}
                      onChange={(e) => setCreatorCron(e.target.value)}
                      required 
                    />
                    <span className="form-help">Uses standard 5-field expression format</span>
                  </div>
                )}

                {creatorType === 'batch' && (
                  <div className="form-group">
                    <label>Batch Size (Jobs count)</label>
                    <input 
                      type="number" 
                      className="glass-input" 
                      min="1" max="100"
                      value={creatorBatchCount}
                      onChange={(e) => setCreatorBatchCount(e.target.value)}
                      required 
                    />
                  </div>
                )}

                {creatorType === 'dependency' && (
                  <div className="form-group">
                    <label>Parent Job ID</label>
                    <input 
                      type="text" 
                      className="glass-input" 
                      placeholder="Paste parent job UUID here..."
                      value={creatorParentJobId}
                      onChange={(e) => setCreatorParentJobId(e.target.value)}
                      required 
                    />
                    <span className="form-help">Job will execute ONLY after parent job completes.</span>
                  </div>
                )}

                <div className="form-group full-width">
                  <label>JSON Payload Configuration</label>
                  <textarea 
                    className="glass-input" 
                    rows="6"
                    style={{ fontFamily: 'JetBrains Mono', fontSize: '13px', resize: 'vertical' }}
                    value={creatorPayload}
                    onChange={(e) => setCreatorPayload(e.target.value)}
                    required
                  />
                  <span className="form-help">Configure task run duration, simulated failure triggers, and exception details</span>
                </div>
              </div>

              <button type="submit" className="glass-button primary" style={{ width: '100%', marginTop: '10px' }}>
                Enqueue Background Job
              </button>
            </form>
          </div>
        )}

        {activeTab === 'explorer' && (
          <div className="glass-panel" style={{ padding: '30px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
              <h3>Job Explorer</h3>
              <button className="glass-button" onClick={fetchJobs} style={{ padding: '8px 12px', fontSize: '12px' }}>
                Refresh
              </button>
            </div>

            {/* Filter controls */}
            <div className="explorer-controls">
              <input 
                type="text" 
                className="glass-input" 
                placeholder="Search job logs / names..."
                value={explorerSearch}
                onChange={(e) => setExplorerSearch(e.target.value)}
              />
              
              <select 
                className="glass-input"
                value={explorerFilterQueue}
                onChange={(e) => setExplorerFilterQueue(e.target.value)}
              >
                <option value="">All Queues</option>
                {queues.map(q => (
                  <option key={q.id} value={q.id}>{q.name}</option>
                ))}
              </select>

              <select 
                className="glass-input"
                value={explorerFilterStatus}
                onChange={(e) => setExplorerFilterStatus(e.target.value)}
              >
                <option value="">All Statuses</option>
                <option value="queued">Queued</option>
                <option value="scheduled">Scheduled</option>
                <option value="claimed">Claimed</option>
                <option value="running">Running</option>
                <option value="completed">Completed</option>
                <option value="failed">Failed / DLQ</option>
                <option value="cancelled">Cancelled</option>
              </select>
            </div>

            {/* Table */}
            <div className="jobs-table-container">
              <table className="jobs-table">
                <thead>
                  <tr>
                    <th>Job Action</th>
                    <th>Target Queue</th>
                    <th>Status</th>
                    <th>Scheduled Run</th>
                    <th>Attempts</th>
                    <th>Payload</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.length === 0 ? (
                    <tr>
                      <td colSpan="6" style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '30px' }}>
                        No jobs matched current filters
                      </td>
                    </tr>
                  ) : (
                    jobs.map(j => (
                      <tr key={j.id} onClick={() => setInspectedJobId(j.id)}>
                        <td>
                          <strong>{j.name}</strong>
                          <span style={{ fontSize: '10px', display: 'block', color: 'var(--text-muted)' }}>{j.id}</span>
                        </td>
                        <td>{j.queue_name}</td>
                        <td>
                          <span className={`status-badge ${j.status}`}>
                            {j.status}
                          </span>
                        </td>
                        <td>{new Date(j.run_at).toLocaleString()}</td>
                        <td>{j.attempt_number}</td>
                        <td className="jobs-table-payload">{j.payload}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            <div className="pagination">
              <span style={{ fontSize: '13px', color: 'var(--text-secondary)' }}>
                Page {explorerPage} of {explorerTotalPages || 1}
              </span>
              <div style={{ display: 'flex', gap: '8px' }}>
                <button 
                  className="glass-button" 
                  disabled={explorerPage <= 1}
                  onClick={() => setExplorerPage(explorerPage - 1)}
                  style={{ padding: '6px 12px' }}
                >
                  Prev
                </button>
                <button 
                  className="glass-button" 
                  disabled={explorerPage >= explorerTotalPages}
                  onClick={() => setExplorerPage(explorerPage + 1)}
                  style={{ padding: '6px 12px' }}
                >
                  Next
                </button>
              </div>
            </div>
          </div>
        )}
      </main>

      {/* Inspector Sidebar Drawer */}
      {inspectedJobId && inspectedJobData && (
        <div className="modal-overlay" onClick={() => { setInspectedJobId(null); setInspectedJobData(null); }}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <div>
                <h2>Inspect Job</h2>
                <span style={{ fontSize: '12px', color: 'var(--text-muted)', fontFamily: 'monospace' }}>
                  UUID: {inspectedJobId}
                </span>
              </div>
              <button className="glass-button" onClick={() => { setInspectedJobId(null); setInspectedJobData(null); }}>
                Close
              </button>
            </div>

            <div>
              <h3>Metadata</h3>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px', marginTop: '10px', fontSize: '14px' }}>
                <div><strong>Action Name:</strong> {inspectedJobData.job.name}</div>
                <div>
                  <strong>Status:</strong>{' '}
                  <span className={`status-badge ${inspectedJobData.job.status}`}>
                    {inspectedJobData.job.status}
                  </span>
                </div>
                <div><strong>Queue:</strong> {inspectedJobData.job.queue_name}</div>
                <div><strong>Attempts Count:</strong> {inspectedJobData.job.attempt_number}</div>
                {inspectedJobData.job.cron_expression && (
                  <div style={{ gridColumn: 'span 2' }}>
                    <strong>Cron Schedule:</strong> <code>{inspectedJobData.job.cron_expression}</code>
                  </div>
                )}
              </div>
            </div>

            {/* Parent Dependencies */}
            {inspectedJobData.dependencies && inspectedJobData.dependencies.length > 0 && (
              <div>
                <h3>Parent Job Dependencies</h3>
                <div className="dependencies-list">
                  {inspectedJobData.dependencies.map(d => (
                    <span key={d.parent_job_id} className="dependency-tag">
                      <strong>{d.name}</strong> 
                      <span className={`status-badge ${d.status}`} style={{ fontSize: '9px', padding: '2px 4px' }}>
                        {d.status}
                      </span>
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* AI Diagnostics Summary */}
            {(inspectedJobData.job.status === 'failed' || inspectedJobData.job.is_dlq || inspectedJobData.job.error_message) && (
              <div style={{ marginTop: '20px', background: 'rgba(239, 68, 68, 0.03)', border: '1px dashed rgba(239, 68, 68, 0.25)', borderRadius: '12px', padding: '16px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
                  <span style={{ fontSize: '18px' }}>🤖</span>
                  <h3 style={{ margin: 0, color: 'var(--color-danger)', fontSize: '14px', textTransform: 'uppercase', letterSpacing: '0.5px' }}>AI Diagnostic Summary</h3>
                </div>
                {loadingAiSummary ? (
                  <div style={{ color: 'var(--text-muted)', fontSize: '13px' }}>Analyzing execution trace logs, compiling suggestions...</div>
                ) : aiAnalysisSummary ? (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', fontSize: '13px' }}>
                    <div>
                      <strong style={{ color: 'var(--text-primary)', display: 'block', marginBottom: '4px' }}>Diagnosis Summary:</strong>
                      <span style={{ color: 'var(--text-secondary)' }}>{aiAnalysisSummary.summary}</span>
                    </div>
                    <div>
                      <strong style={{ color: 'var(--text-primary)', display: 'block', marginBottom: '4px' }}>Suggested Action:</strong>
                      <span style={{ color: 'var(--color-primary)', fontWeight: '500' }}>{aiAnalysisSummary.suggestions}</span>
                    </div>
                    {aiAnalysisSummary.rawError && (
                      <div style={{ background: 'rgba(0,0,0,0.3)', padding: '8px', borderRadius: '6px', fontFamily: 'monospace', fontSize: '11px', color: 'var(--text-muted)', border: '1px solid rgba(255,255,255,0.05)', overflowX: 'auto', marginTop: '6px' }}>
                        {aiAnalysisSummary.rawError}
                      </div>
                    )}
                  </div>
                ) : (
                  <div style={{ color: 'var(--text-muted)', fontSize: '13px' }}>Failure logs not analyzed.</div>
                )}
              </div>
            )}

            {/* Payload */}
            <div>
              <h3>Payload Configuration</h3>
              <pre style={{ background: '#050505', border: '1px solid var(--border-color)', borderRadius: '8px', padding: '16px', fontSize: '12px', fontFamily: 'JetBrains Mono', overflowX: 'auto', marginTop: '10px' }}>
                {JSON.stringify(JSON.parse(inspectedJobData.job.payload), null, 2)}
              </pre>
            </div>

            {/* Action controls */}
            <div style={{ display: 'flex', gap: '12px' }}>
              {(inspectedJobData.job.status === 'failed' || inspectedJobData.job.is_dlq || inspectedJobData.job.status === 'cancelled') && (
                <button 
                  className="glass-button success" 
                  style={{ flex: 1 }}
                  onClick={() => handleRetryJob(inspectedJobId)}
                >
                  Retry Job (Requeue)
                </button>
              )}
              {['queued', 'scheduled'].includes(inspectedJobData.job.status) && (
                <button 
                  className="glass-button danger" 
                  style={{ flex: 1 }}
                  onClick={() => handleCancelJob(inspectedJobId)}
                >
                  Cancel Job Execution
                </button>
              )}
            </div>

            {/* Log Output */}
            <div>
              <h3>Telemetry Log Stream</h3>
              <div className="logs-viewer" style={{ marginTop: '10px' }}>
                {inspectedJobData.logs.length === 0 ? (
                  <div style={{ color: 'var(--text-muted)' }}>No logs emitted yet for this job.</div>
                ) : (
                  inspectedJobData.logs.map((log, index) => (
                    <div key={index} className={`log-entry ${log.level}`}>
                      [{new Date(log.timestamp).toLocaleTimeString()}] {log.message}
                    </div>
                  ))
                )}
              </div>
            </div>

            {/* Executions Attempts List */}
            <div>
              <h3>Execution History</h3>
              <div className="executions-list" style={{ marginTop: '10px' }}>
                {inspectedJobData.executions.length === 0 ? (
                  <div style={{ color: 'var(--text-muted)', fontSize: '13px' }}>
                    No executions attempts recorded yet.
                  </div>
                ) : (
                  inspectedJobData.executions.map(exec => (
                    <div key={exec.id} className="execution-item">
                      <div className="execution-header">
                        <span>Attempt #{exec.attempt_number} ({exec.worker_id})</span>
                        <span className={`status-badge ${exec.status === 'completed' ? 'completed' : 'failed'}`}>
                          {exec.status}
                        </span>
                      </div>
                      <div style={{ color: 'var(--text-secondary)' }}>
                        Duration: {exec.duration_ms}ms • Finished: {new Date(exec.finished_at).toLocaleString()}
                      </div>
                      {exec.error_message && (
                        <div className="execution-error">{exec.error_message}</div>
                      )}
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
