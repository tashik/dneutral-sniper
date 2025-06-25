// Type definitions for lightweight-charts

declare module 'lightweight-charts' {
  export type ColorType = 0 | 1 | 2;
  
  export interface LineData {
    time: string;
    value: number;
  }
  
  export interface IChartApi {
    applyOptions(options: any): void;
    timeScale(): ITimeScaleApi;
    addSeries(type: 'Line', options?: SeriesOptions): ISeriesApi<'Line'>;
    remove(): void;
    removeSeries(series: ISeriesApi<any>): void;
  }
  
  export interface ITimeScaleApi {
    fitContent(): void;
  }
  
  export interface ISeriesApi<T> {
    setData(data: LineData[]): void;
    update(bar: any): void;
  }
  
  export interface SeriesOptions {
    color?: string;
    lineWidth?: number;
    priceLineVisible?: boolean;
    lastValueVisible?: boolean;
    priceLineColor?: string;
    priceLineWidth?: number;
  }
  
  export function createChart(container: HTMLElement, options?: any): IChartApi;
}
