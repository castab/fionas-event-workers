import type { InquirySubmitted } from '../../../packages/events/src/index.ts';
export const receipt = { id: 'c755f7cd-1e28-4c75-a85f-d066ede7387d', createdAt: '2026-10-14T18:30:00.321012Z' };
export function fixture(): InquirySubmitted {
  return {
    schemaVersion: 1, type: 'fionas.inquiry.submitted',
    id: '0f8b5a3e-6c2d-4f1a-9b7e-3d4c5b6a7e8f',
    occurredAt: '2026-10-09T18:22:03.123Z', source: 'fionas-web',
    data: {
      name: 'Jane Doe', email: 'jane@example.com', message: 'Backyard birthday',
      zipCode: '93720', eventDate: '2026-12-05', eventType: 'BIRTHDAY',
      requestedService: {
        guestCount: 75, guestCountIsMinimum: false,
        items: [
          { label: 'Chocolate Chip', group: 'Hand-scooped', key: 'hand-scooped-chocolate-chip' },
          { label: 'Rainbow Sprinkles', group: 'Toppings', key: 'rainbow-sprinkles' },
          { label: 'Sugar Cones', group: 'Cones & cups', key: 'sugar-cone' },
        ],
        pricingReference: 'web-menu-1:synthetic-1',
      },
      priceRevision: 'synthetic-1',
      lines: [
        { description: 'Base service', unitPrice: '101.00', taxAmount: '0.00', currency: 'USD' },
        { description: 'Ice cream service', subDescription: '75 guests', quantity: '75', unitPrice: '7.00', taxAmount: '0.00', currency: 'USD' },
      ],
    },
  };
}
export const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
