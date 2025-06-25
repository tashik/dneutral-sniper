// Portfolio and Option types
export interface OptionPosition {
  id: string;
  symbol: string;
  option_type: 'call' | 'put';
  strike: number;
  expiration: string;
  quantity: number;
  premium: number;
  created_at: string;
  updated_at: string;
  delta?: number;
  gamma?: number;
  vega?: number;
  theta?: number;
  iv?: number;
  underlyingPrice?: number;
}

export interface Portfolio {
  id: string;
  name: string;
  description: string | null;
  underlying: string;
  created_at: string;
  updated_at: string;
  options: OptionPosition[];
  total_delta: number;
  total_gamma: number;
  total_vega: number;
  total_theta: number;
  total_value: number;
}

export interface PortfolioStore {
  portfolios: Portfolio[];
  currentPortfolio: Portfolio | null;
  loading: boolean;
  error: string | null;
  init: () => void;
  fetchPortfolios: () => Promise<void>;
  createPortfolio: (name: string, underlying: string, description?: string) => Promise<void>;
  updatePortfolio: (id: string, name: string, underlying: string, description?: string) => Promise<void>;
  deletePortfolio: (id: string) => Promise<void>;
  addOption: (portfolioId: string, option: Omit<OptionPosition, 'id' | 'created_at' | 'updated_at'>) => Promise<void>;
  removeOption: (portfolioId: string, optionId: string) => Promise<void>;
  setCurrentPortfolio: (portfolio: Portfolio | null) => void;
}
