package app

import (
	"context"
	"errors"
	"log"

	"grab-mini/promotion-service/internal/domain"
)

// PromotionView is a promotion as an admin sees it: with its status as of now.
type PromotionView struct {
	domain.Summary
	Status string
}

func (s *Service) view(sum domain.Summary, status string) PromotionView {
	return PromotionView{Summary: sum, Status: status}
}

// loadView reads one promotion with its usage and status.
func (s *Service) loadView(ctx context.Context, code string) (PromotionView, error) {
	sum, err := s.d.Views.View(ctx, code)
	if err != nil {
		return PromotionView{}, step("load promotion", err)
	}
	return s.view(sum, domain.Status(sum.Promotion, s.d.Now())), nil
}

// CreatePromotionCommand is an admin's request for a new promotion.
type CreatePromotionCommand struct {
	AdminID string
	Spec    domain.PromotionSpec
}

// CreatePromotion stores the promotion the spec describes. Errors:
// *domain.InvalidPromotionError, domain.ErrCodeExists, else *StepError.
func (s *Service) CreatePromotion(ctx context.Context, c CreatePromotionCommand) (PromotionView, error) {
	p, err := domain.NewPromotion(c.Spec, s.d.Now())
	if err != nil {
		return PromotionView{}, err
	}
	if err := s.d.Writer.Create(ctx, p, c.AdminID); err != nil {
		if errors.Is(err, domain.ErrCodeExists) {
			return PromotionView{}, domain.ErrCodeExists
		}
		return PromotionView{}, step("create promotion", err)
	}
	v, err := s.loadView(ctx, p.Code)
	if err != nil {
		return PromotionView{}, err
	}
	log.Printf("promotion %s created by %s", p.Code, c.AdminID)
	return v, nil
}

// ListPromotions returns every promotion, newest first; never nil.
func (s *Service) ListPromotions(ctx context.Context) ([]PromotionView, error) {
	sums, err := s.d.Views.List(ctx)
	if err != nil {
		// A row that failed to scan already names its step ("read promotion"),
		// as the flat handler logged it; keep that label.
		var se *StepError
		if errors.As(err, &se) {
			return nil, err
		}
		return nil, step("list promotions", err)
	}
	now := s.d.Now()
	views := make([]PromotionView, 0, len(sums))
	for _, sum := range sums {
		views = append(views, s.view(sum, domain.Status(sum.Promotion, now)))
	}
	return views, nil
}

// DisablePromotion disables the code. Doing it twice is a success and keeps
// the first time; domain.ErrNotFound when no promotion has the code.
func (s *Service) DisablePromotion(ctx context.Context, adminID, code string) (PromotionView, error) {
	code = domain.NormalizeCode(code)
	found, err := s.d.Writer.Disable(ctx, code, s.d.Now())
	if err != nil {
		return PromotionView{}, step("disable promotion", err)
	}
	if !found {
		return PromotionView{}, domain.ErrNotFound
	}
	v, err := s.loadView(ctx, code)
	if err != nil {
		return PromotionView{}, err
	}
	log.Printf("promotion %s disabled by %s", code, adminID)
	return v, nil
}
