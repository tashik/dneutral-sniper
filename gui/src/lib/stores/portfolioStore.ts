import { writable } from 'svelte/store';
import { io, type Socket } from 'socket.io-client';
import type { Writable } from 'svelte/store';

// Import and export types
import type { Portfolio, OptionPosition } from '$types/portfolio';

export type { Portfolio, OptionPosition };

// API response types
interface ApiResponse<T> {
  data?: T;
  error?: string;
}

// Socket event types
interface PortfolioUpdateEvent {
  portfolio_id: string;
  data: Portfolio;
}

// Store state interface
interface PortfolioStoreState {
  portfolios: Portfolio[];
  currentPortfolio: Portfolio | null;
  loading: boolean;
  error: string | null;
}

// Define the store methods interface
interface IPortfolioStore extends Writable<PortfolioStoreState> {
  init: () => void;
  fetchPortfolios: () => Promise<void>;
  createPortfolio: (name: string, underlying: string, description?: string) => Promise<Portfolio>;
  updatePortfolio: (id: string, name: string, underlying: string, description?: string) => Promise<Portfolio>;
  deletePortfolio: (id: string) => Promise<void>;
  addOption: (portfolioId: string, option: Omit<OptionPosition, 'id' | 'created_at' | 'updated_at'>) => Promise<void>;
  removeOption: (portfolioId: string, optionId: string) => Promise<void>;
  setCurrentPortfolio: (portfolio: Portfolio | null) => void;
}

// Initialize store with proper typing
const { subscribe, set, update } = writable<PortfolioStoreState>({
  portfolios: [],
  currentPortfolio: null,
  loading: false,
  error: null
});

// API URL - Include the /api/v1 prefix to match the backend routes
const API_URL = 'http://localhost:8000/api/v1';

// WebSocket URL - Use FastAPI's native WebSocket endpoint (no /api/v1 prefix for WebSocket)
const WS_URL = API_URL.replace('/api/v1', '').replace('http', 'ws');

// WebSocket connection
let socket: WebSocket | null = null;

// Helper function to handle API errors
async function handleApiResponse<T>(response: Response): Promise<T> {
  const data: ApiResponse<T> = await response.json();
  if (!response.ok || data.error) {
    throw new Error(data.error || `HTTP error! status: ${response.status}`);
  }
  if (!data.data) {
    throw new Error('No data received from server');
  }
  return data.data;
}

// Initialize WebSocket connection
function initWebSocket() {
  if (socket) {
    // Close existing connection if it exists
    if (socket.readyState === WebSocket.OPEN) return;
    socket.close();
  }

  // Create new WebSocket connection
  socket = new WebSocket(`${WS_URL}/ws`);

  socket.onopen = () => {
    console.log('Connected to WebSocket server');
  };

  socket.onclose = () => {
    console.log('Disconnected from WebSocket server');
    // Attempt to reconnect after a delay
    setTimeout(initWebSocket, 5000);
  };

  socket.onmessage = (event) => {
    try {
      const message = JSON.parse(event.data);

      if (message.type === 'portfolio_updated') {
        const event = message as PortfolioUpdateEvent;
        update(state => {
          // Check if this is a new portfolio or an update
          const existingIndex = state.portfolios.findIndex(p => p.id === event.portfolio_id);
          const updatedPortfolios = [...state.portfolios];

          if (existingIndex !== -1) {
            // Update existing portfolio
            updatedPortfolios[existingIndex] = event.data;
          } else {
            // Add new portfolio
            updatedPortfolios.push(event.data);
          }

          // Update current portfolio if it's the one being updated
          const currentPortfolio = state.currentPortfolio?.id === event.portfolio_id
            ? event.data
            : state.currentPortfolio;

          return {
            ...state,
            portfolios: updatedPortfolios,
            currentPortfolio,
            loading: false
          };
        });
      }
    } catch (error) {
      console.error('Error processing WebSocket message:', error);
    }
  };

  socket.onerror = (error) => {
    console.error('WebSocket error:', error);
  };
}

// Initialize store
function init() {
  fetchPortfolios().catch(error => {
    console.error('Failed to initialize store:', error);
    update(state => ({
      ...state,
      error: error.message,
      loading: false
    }));
  });
  initWebSocket();
}

// Fetch all portfolios
async function fetchPortfolios() {
  update(state => ({ ...state, loading: true, error: null }));
  try {
    const response = await fetch(`${API_URL}/portfolios`);
    const portfolios = await handleApiResponse<Portfolio[]>(response);
    update(state => ({
      ...state,
      portfolios,
      loading: false,
      error: null
    }));
  } catch (error) {
    console.error('Failed to fetch portfolios:', error);
    update(state => ({
      ...state,
      error: error instanceof Error ? error.message : 'Failed to fetch portfolios',
      loading: false
    }));
    throw error;
  }
}

// Create new portfolio
async function createPortfolio(name: string, underlying: string, description: string = ''): Promise<Portfolio> {
  update(state => ({ ...state, loading: true, error: null }));
  try {
    const response = await fetch(`${API_URL}/portfolios`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name,
        underlying,
        description: description || undefined
      })
    });

    // If the response is empty but successful, rely on WebSocket update
    if (response.status === 200 && response.headers.get('content-length') === '0') {
      // Just return a minimal portfolio object - the WebSocket update will handle the rest
      const minimalPortfolio: Portfolio = {
        id: 'temporary-id', // This will be updated by the WebSocket
        name,
        underlying,
        description: description || null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        options: [],
        total_delta: 0,
        total_gamma: 0,
        total_vega: 0,
        total_theta: 0,
        total_value: 0
      };

      update(state => ({
        ...state,
        loading: false
      }));

      return minimalPortfolio;
    }

    // If there's content, try to parse it as JSON
    if (response.headers.get('content-type')?.includes('application/json')) {
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || `HTTP error! status: ${response.status}`);
      }

      // Update the local state with the new portfolio
      update(state => {
        const existingIndex = state.portfolios.findIndex(p => p.id === data.id);
        const updatedPortfolios = [...state.portfolios];

        if (existingIndex >= 0) {
          updatedPortfolios[existingIndex] = data;
        } else {
          updatedPortfolios.push(data);
        }

        return {
          ...state,
          portfolios: updatedPortfolios,
          loading: false
        };
      });

      return data;
    }

    // If we get here, the response wasn't JSON and wasn't empty
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    // If we get here, we have a successful non-JSON response
    // Rely on WebSocket for the actual update
    update(state => ({
      ...state,
      loading: false
    }));

    // Return a minimal portfolio object
    const minimalPortfolio: Portfolio = {
      id: 'temporary-id',
      name,
      underlying,
      description: description || null,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      options: [],
      total_delta: 0,
      total_gamma: 0,
      total_vega: 0,
      total_theta: 0,
      total_value: 0
    };

    return minimalPortfolio;
  } catch (error) {
    console.error('Failed to create portfolio:', error);
    update(state => ({
      ...state,
      error: error instanceof Error ? error.message : 'Failed to create portfolio',
      loading: false
    }));
    throw error;
  }
}

// Update portfolio
async function updatePortfolio(
  id: string,
  name: string,
  underlying: string,
  description: string = '',
): Promise<Portfolio> {
  update(state => ({ ...state, loading: true, error: null }));
  try {
    const response = await fetch(`${API_URL}/portfolios/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name,
        description: description || undefined,
        underlying
      })
    });

    // If the response is empty but successful, rely on WebSocket update
    if (response.status === 200 && response.headers.get('content-length') === '0') {
      // Create a minimal updated portfolio object - the WebSocket update will handle the rest
      const minimalPortfolio: Portfolio = {
        id,
        name,
        underlying,
        description: description || null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        options: [],
        total_delta: 0,
        total_gamma: 0,
        total_vega: 0,
        total_theta: 0,
        total_value: 0
      };

      update(state => ({
        ...state,
        loading: false
      }));

      return minimalPortfolio;
    }

    // If there's content, try to parse it as JSON
    if (response.headers.get('content-type')?.includes('application/json')) {
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || `HTTP error! status: ${response.status}`);
      }

      // Update the local state with the updated portfolio
      update(state => {
        const updatedPortfolios = state.portfolios.map(p =>
          p.id === id ? { ...p, ...data } : p
        );

        // Update current portfolio if it's the one being updated
        const currentPortfolio = state.currentPortfolio?.id === id
          ? { ...state.currentPortfolio, ...data }
          : state.currentPortfolio;

        return {
          ...state,
          portfolios: updatedPortfolios,
          currentPortfolio,
          loading: false
        };
      });

      return data;
    }

    // If we get here, the response wasn't JSON and wasn't empty
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    // If we get here, we have a successful non-JSON response
    // Rely on WebSocket for the actual update
    update(state => ({
      ...state,
      loading: false
    }));

    // Return a minimal portfolio object
    return {
      id,
      name,
      underlying,
      description: description || null,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      options: [],
      total_delta: 0,
      total_gamma: 0,
      total_vega: 0,
      total_theta: 0,
      total_value: 0
    };
  } catch (error) {
    console.error('Failed to update portfolio:', error);
    update(state => ({
      ...state,
      error: error instanceof Error ? error.message : 'Failed to update portfolio',
      loading: false
    }));
    throw error;
  }
}

// Delete portfolio
async function deletePortfolio(id: string): Promise<void> {
  update(state => ({ ...state, loading: true, error: null }));
  try {
    const response = await fetch(`${API_URL}/portfolios/${id}`, {
      method: 'DELETE'
    });

    // Check if the response is successful but has no content (204)
    if (response.status === 204) {
      // Update the UI immediately by removing the deleted portfolio
      update(state => ({
        ...state,
        portfolios: state.portfolios.filter(p => p.id !== id),
        currentPortfolio: state.currentPortfolio?.id === id ? null : state.currentPortfolio,
        loading: false
      }));
      return;
    }

    // If there's content, try to parse it as JSON
    if (response.headers.get('content-type')?.includes('application/json')) {
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || `HTTP error! status: ${response.status}`);
      }
      // If we have data, update the UI
      update(state => ({
        ...state,
        portfolios: state.portfolios.filter(p => p.id !== id),
        currentPortfolio: state.currentPortfolio?.id === id ? null : state.currentPortfolio,
        loading: false
      }));
      return;
    }

    // If we get here, the response wasn't 204 and didn't contain JSON
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

  } catch (error) {
    console.error('Failed to delete portfolio:', error);
    update(state => ({
      ...state,
      error: error instanceof Error ? error.message : 'Failed to delete portfolio',
      loading: false
    }));
    throw error;
  }
}

// Add option to portfolio
async function addOption(
  portfolioId: string,
  optionData: Omit<OptionPosition, 'id' | 'created_at' | 'updated_at'>
): Promise<void> {
  update(state => ({ ...state, loading: true, error: null }));
  try {
    const response = await fetch(`${API_URL}/portfolios/${portfolioId}/options`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(optionData)
    });
    await handleApiResponse<OptionPosition>(response);
    await fetchPortfolios();
  } catch (error) {
    console.error('Failed to add option:', error);
    update(state => ({
      ...state,
      error: error instanceof Error ? error.message : 'Failed to add option',
      loading: false
    }));
    throw error;
  }
}

// Remove option from portfolio
async function removeOption(portfolioId: string, optionId: string): Promise<void> {
  update(state => ({ ...state, loading: true, error: null }));
  try {
    const response = await fetch(
      `${API_URL}/portfolios/${portfolioId}/options/${optionId}`,
      { method: 'DELETE' }
    );
    await handleApiResponse<{ success: boolean }>(response);
    await fetchPortfolios();
  } catch (error) {
    console.error('Failed to remove option:', error);
    update(state => ({
      ...state,
      error: error instanceof Error ? error.message : 'Failed to remove option',
      loading: false
    }));
    throw error;
  }
}

// Set current portfolio
function setCurrentPortfolio(portfolio: Portfolio | null): void {
  update(state => ({
    ...state,
    currentPortfolio: portfolio,
    error: null
  }));
}

// Create the store with all required methods
export const portfolioStore: IPortfolioStore = {
  // Writable methods
  subscribe,
  set: (value: PortfolioStoreState) => set(value),
  update: (updater: (value: PortfolioStoreState) => PortfolioStoreState) => update(updater),

  // Custom store methods
  init,
  fetchPortfolios,
  createPortfolio,
  updatePortfolio,
  deletePortfolio,
  addOption,
  removeOption,
  setCurrentPortfolio
};
